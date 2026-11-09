package worker

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/disgoorg/disgo"
	"github.com/disgoorg/disgo/bot"
	"github.com/disgoorg/disgo/cache"
	"github.com/disgoorg/disgo/rest"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/PurgeBot-net/common/job"
	"github.com/PurgeBot-net/common/log"
	"github.com/PurgeBot-net/database"
	"github.com/PurgeBot-net/purger/config"
	"github.com/PurgeBot-net/purger/internal/engine"
	"github.com/PurgeBot-net/purger/internal/ratelimit"
)

type Worker struct {
	cfg     config.Config
	logger  *zap.Logger
	db      *database.Database
	redis   *redis.Client
	client  *bot.Client
	running atomic.Int64
}

func New(cfg config.Config, logger *zap.Logger, db *database.Database, redis *redis.Client) (*Worker, error) {
	limiter := ratelimit.New(logger)
	// disgo's default; stated so it cannot silently change.
	client, err := disgo.New(cfg.Token,
		bot.WithCacheConfigOpts(cache.WithCaches(cache.FlagsNone)),
		bot.WithLogger(log.Slog(logger)),
		bot.WithRestClientConfigOpts(rest.WithRateLimiter(limiter)),
	)
	if err != nil {
		return nil, fmt.Errorf("create discord client: %w", err)
	}
	return &Worker{cfg: cfg, logger: logger, db: db, redis: redis, client: client}, nil
}

// Snowflakes exceed 2^53, so a JSON reader using float64 corrupts them as numbers.
func idField(key string, id uint64) zap.Field {
	return zap.String(key, strconv.FormatUint(id, 10))
}

// Run blocks, spawning WorkerConcurrency goroutines that consume purge jobs
// from Redis concurrently. Returns when ctx is cancelled and all goroutines exit.
func (w *Worker) Run(ctx context.Context) {
	w.recoverActiveJobs(ctx)

	concurrency := w.cfg.WorkerConcurrency
	if concurrency < 1 {
		concurrency = 1
	}
	w.logger.Info("purge worker started",
		zap.Int("concurrency", concurrency),
		zap.Int("channel_concurrency", w.cfg.ChannelConcurrency),
	)

	var wg sync.WaitGroup
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.loop(ctx)
		}()
	}
	wg.Wait()
	w.logger.Info("purge worker stopped", zap.Int64("running", w.running.Load()))
}

// loop is the per-goroutine dequeue-and-execute cycle.
func (w *Worker) loop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		j, err := job.Dequeue(ctx, w.redis, 5*time.Second)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.logger.Error("dequeue job", zap.Error(err))
			continue
		}
		if j == nil {
			continue // timeout, loop again
		}
		jobLog := w.logger.With(zap.String("job_id", j.ID), idField("guild_id", j.GuildID))

		// Guard against double-enqueue: skip if this is a stale copy of a recovered job.
		active, err := job.GetActiveJob(ctx, w.redis, j.GuildID)
		if err != nil {
			jobLog.Error("verify active job, dropping until restart recovery", zap.Error(err))
			continue
		}
		if active == nil {
			jobLog.Info("skipping stale recovered job", zap.String("reason", "guild has no active job"))
			continue
		}
		if active.ID != j.ID {
			jobLog.Info("skipping stale recovered job",
				zap.String("reason", "superseded"),
				zap.String("active_job_id", active.ID),
			)
			continue
		}

		running := w.running.Add(1)
		jobLog.Info("processing purge job",
			zap.String("purge_type", string(j.PurgeType)),
			zap.String("target_type", string(j.TargetType)),
			idField("target_id", j.TargetID),
			idField("requested_by_id", j.RequestedByID),
			zap.Duration("queue_wait", time.Since(j.CreatedAt)),
			zap.Int64("running", running),
		)

		eng := engine.New(w.cfg, jobLog, w.db, w.redis, w.client)
		// The active key is authoritative; a queued copy re-marshalled by an older build can lack fields.
		err = eng.Execute(ctx, active)
		// Not deferred: extracting a function to reach one would drag the cleanup
		// below into the interrupt path, which must skip it.
		running = w.running.Add(-1)

		if errors.Is(err, engine.ErrInterrupted) {
			jobLog.Info("purge job interrupted; leaving active for recovery", zap.Int64("running", running))
			return
		}

		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err != nil {
			if errors.Is(err, engine.ErrCancelled) {
				jobLog.Info("purge job cancelled", zap.Int64("running", running))
			} else {
				jobLog.Error("purge job failed", zap.Int64("running", running), zap.Error(err))
			}
		}
		job.DeleteProgress(cleanupCtx, w.redis, j.ID)
		job.DeleteActiveJob(cleanupCtx, w.redis, j.GuildID)
		cancel()
	}
}

// recoverActiveJobs re-queues any jobs that were active when the worker last crashed.
func (w *Worker) recoverActiveJobs(ctx context.Context) {
	jobs, err := job.GetAllActiveJobs(ctx, w.redis)
	if err != nil {
		w.logger.Error("scan active jobs for recovery", zap.Error(err))
		return
	}
	w.logger.Info("scanned active jobs for recovery", zap.Int("jobs", len(jobs)))
	for _, j := range jobs {
		jobLog := w.logger.With(zap.String("job_id", j.ID), idField("guild_id", j.GuildID))
		if err := job.RemoveQueued(ctx, w.redis, j); err != nil {
			jobLog.Warn("remove stale queued recovered job, duplicate queue entry expected", zap.Error(err))
		}
		if err := job.Enqueue(ctx, w.redis, j); err != nil {
			jobLog.Error("re-enqueue recovered job", zap.Error(err))
		} else {
			jobLog.Info("recovered active job",
				zap.String("purge_type", string(j.PurgeType)),
				zap.Duration("age", time.Since(j.CreatedAt)),
			)
		}
	}
}
