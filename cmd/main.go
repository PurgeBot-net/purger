package main

import (
	"context"
	"os"
	"os/signal"
	"strconv"
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

	logger, err := log.New(cfg.LogLevel, cfg.LogJSON)
	if err != nil {
		panic("create logger: " + err.Error())
	}
	logger = log.WithSentry(logger, cfg.SentryDSN)
	defer logger.Sync()

	// cfg.DatabaseURL() is never logged: it embeds the password.
	logger.Info("purger starting",
		zap.String("application_id", strconv.FormatUint(cfg.ApplicationID, 10)),
		zap.Int("worker_concurrency", cfg.WorkerConcurrency),
		zap.Int("channel_concurrency", cfg.ChannelConcurrency),
		zap.String("log_level", cfg.LogLevel),
		zap.Bool("log_json", cfg.LogJSON),
		zap.Bool("sentry", cfg.SentryDSN != ""),
		zap.String("redis_addr", cfg.RedisAddr),
		zap.Int("redis_db", cfg.RedisDB),
		zap.String("database", cfg.DatabaseName),
	)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	db, err := database.New(ctx, cfg.DatabaseURL())
	if err != nil {
		logger.Fatal("connect database",
			zap.Error(err),
			zap.String("host", cfg.DatabaseHost),
			zap.Int("port", cfg.DatabasePort),
			zap.String("database", cfg.DatabaseName),
			zap.String("user", cfg.DatabaseUser),
		)
	}
	defer db.Close()

	redis, err := rdb.New(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	if err != nil {
		logger.Fatal("connect redis", zap.Error(err), zap.String("addr", cfg.RedisAddr), zap.Int("db", cfg.RedisDB))
	}
	defer redis.Close()

	w, err := worker.New(cfg, logger, db, redis)
	if err != nil {
		logger.Fatal("create worker", zap.Error(err))
	}
	w.Run(ctx)
	logger.Info("purger shutdown complete")
}
