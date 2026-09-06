# purger

Purge worker service for PurgeBot. Consumes jobs from the Redis queue and performs message deletion on Discord.

## Responsibilities

- Dequeues purge jobs from Redis
- Resolves the target (server, category, or channel) into a list of channels and threads
- Fetches messages in batches and applies filters (date, content, author type)
- Bulk-deletes messages newer than 14 days; individually deletes older ones
- Updates the original interaction response with live progress and a cancel button
- Records completed purge jobs to the database for stats

## Configuration

All configuration is loaded from environment variables (see `.env.example` in the docker repo).

| Variable                                   | Description                                                |
| ------------------------------------------ | ---------------------------------------------------------- |
| `DISCORD_TOKEN`                            | Bot token                                                  |
| `DISCORD_APPLICATION_ID`                   | Application ID                                             |
| `DATABASE_*`                               | PostgreSQL connection                                      |
| `REDIS_ADDR`, `REDIS_PASSWORD`, `REDIS_DB` | Redis connection                                           |
| `WORKER_CONCURRENCY`                       | Purge workers to run in parallel (default `4`)             |
| `PURGE_CHANNEL_CONCURRENCY`                | Channels of one purge to delete from at once (default `3`) |
| `SENTRY_DSN`                               | Sentry error reporting (optional)                          |
| `LOG_LEVEL`                                | `debug`, `info`, `warn`, `error`                           |
| `LOG_JSON`                                 | `true` for JSON log output                                 |
