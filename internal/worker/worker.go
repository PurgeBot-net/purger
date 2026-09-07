package worker

import (
	"context"
	"errors"
	"fmt"
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
		bot.WithRestClientConfigOpts(rest.WithRateLimiter(limiter)),
	)
	if err != nil {
		return nil, fmt.Errorf("create discord client: %w", err)
	}
	return &Worker{cfg: cfg, logger: logger, db: db, redis: redis, client: client}, nil
}

// Run blocks, spawning WorkerConcurrency goroutines that consume purge jobs
// from Redis concurrently. Returns when ctx is cancelled and all goroutines exit.
func (w *Worker) Run(ctx context.Context) {
	w.recoverActiveJobs(ctx)

	concurrency := w.cfg.WorkerConcurrency
	if concurrency < 1 {
		concurrency = 1
	}
	w.logger.Info("purge worker started", zap.Int("concurrency", concurrency))

	eng := engine.New(w.cfg, w.logger, w.db, w.redis, w.client)
	var wg sync.WaitGroup
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.loop(ctx, eng)
		}()
	}
	wg.Wait()
	w.logger.Info("purge worker stopped")
}

// loop is the per-goroutine dequeue-and-execute cycle.
func (w *Worker) loop(ctx context.Context, eng *engine.Engine) {
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
		jobLog := w.logger.With(zap.String("id", j.ID), zap.Uint64("guild_id", j.GuildID))

		// Guard against double-enqueue: skip if this is a stale copy of a recovered job.
		active, err := job.GetActiveJob(ctx, w.redis, j.GuildID)
		if err != nil {
			jobLog.Error("verify active job", zap.Error(err))
			continue
		}
		if active == nil || active.ID != j.ID {
			jobLog.Info("skipping stale recovered job")
			continue
		}

		running := w.running.Add(1)
		jobLog.Info("processing purge job",
			zap.String("type", string(j.PurgeType)),
			zap.Int64("running", running),
		)

		err = eng.Execute(ctx, j)
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
	for _, j := range jobs {
		if err := job.RemoveQueued(ctx, w.redis, j); err != nil {
			w.logger.Warn("remove stale queued recovered job", zap.String("id", j.ID), zap.Uint64("guild_id", j.GuildID), zap.Error(err))
		}
		if err := job.Enqueue(ctx, w.redis, j); err != nil {
			w.logger.Error("re-enqueue recovered job", zap.String("id", j.ID), zap.Uint64("guild_id", j.GuildID), zap.Error(err))
		} else {
			w.logger.Info("recovered active job", zap.String("id", j.ID), zap.Uint64("guild_id", j.GuildID))
		}
	}
}
