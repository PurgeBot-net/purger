package config

import (
	"github.com/caarlos0/env/v11"

	"github.com/PurgeBot-net/common/log"
	"github.com/PurgeBot-net/common/rdb"
	"github.com/PurgeBot-net/database"
)

type Config struct {
	// Discord (REST only — no gateway needed)
	Token         string `env:"DISCORD_TOKEN,required"`
	ApplicationID uint64 `env:"DISCORD_APPLICATION_ID,required"`

	// Worker
	WorkerConcurrency int `env:"WORKER_CONCURRENCY" envDefault:"4"`
	// Channels of one purge to run at once, 0 for every channel. Multiplies with WorkerConcurrency.
	ChannelConcurrency int `env:"PURGE_CHANNEL_CONCURRENCY" envDefault:"10"`

	Database database.Config
	Redis    rdb.Config
	Log      log.Config
}

func Load() (Config, error) {
	return env.ParseAs[Config]()
}
