package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/joho/godotenv/autoload"
	"go.uber.org/zap"

	"github.com/PurgeBot-net/common/log"
	"github.com/PurgeBot-net/common/rdb"
	"github.com/PurgeBot-net/database"
	"github.com/PurgeBot-net/purger/config"
	"github.com/PurgeBot-net/purger/internal/worker"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		panic("load config: " + err.Error())
	}

	logger, err := log.New(cfg.Log.Level, cfg.Log.JSON)
	if err != nil {
		panic("create logger: " + err.Error())
	}
	logger = log.WithSentry(logger, cfg.Log.SentryDSN)
	defer func() { _ = logger.Sync() }()

	// cfg.Database.URL() is never logged: it embeds the password.
	logger.Info("purger starting",
		log.ID("application_id", cfg.ApplicationID),
		zap.Int("worker_concurrency", cfg.WorkerConcurrency),
		zap.Int("channel_concurrency", cfg.ChannelConcurrency),
		zap.String("log_level", cfg.Log.Level),
		zap.Bool("log_json", cfg.Log.JSON),
		zap.Bool("sentry", cfg.Log.SentryDSN != ""),
		zap.String("redis_addr", cfg.Redis.Addr),
		zap.Int("redis_db", cfg.Redis.DB),
		zap.String("database", cfg.Database.Name),
	)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	db, err := database.New(ctx, cfg.Database.URL())
	if err != nil {
		logger.Fatal("connect database",
			zap.Error(err),
			zap.String("host", cfg.Database.Host),
			zap.Int("port", cfg.Database.Port),
			zap.String("database", cfg.Database.Name),
			zap.String("user", cfg.Database.User),
		)
	}
	defer db.Close()

	redis, err := rdb.New(cfg.Redis.Addr, cfg.Redis.Password, cfg.Redis.DB)
	if err != nil {
		logger.Fatal("connect redis", zap.Error(err), zap.String("addr", cfg.Redis.Addr), zap.Int("db", cfg.Redis.DB))
	}
	defer func() { _ = redis.Close() }()

	w, err := worker.New(cfg, logger, db, redis)
	if err != nil {
		logger.Fatal("create worker", zap.Error(err))
	}
	w.Run(ctx)
	logger.Info("purger shutdown complete")
}
