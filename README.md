# purger

Purge worker service for PurgeBot. Consumes jobs from the Redis queue and performs message deletion on Discord.

## Responsibilities

- Dequeues purge jobs from Redis
- Resolves the target (server, category, or channel) into a list of channels and threads
- Checks the bot's permissions before each channel and skips one that lacks View Channel, Read Message History or Manage Messages, or Connect for a voice channel
- Fetches messages in batches of 100 and applies filters (purge type, date, text and attachment filter, flagged media, skipped users and messages)
- Bulk-deletes messages newer than 14 days; individually deletes older ones
- Polls the job's cancel key every second and stops the run once it is set
- Checkpoints progress in Redis and, on start, re-queues jobs a crash or shutdown left active so they resume where they stopped
- Keeps one live progress surface per run, moving one way only as each is refused: interaction
  response, channel message, then a direct message to whoever started the purge
- Records completed purge jobs to the database for stats, unless the server turned stats off with `/settings`

## Configuration

All configuration is loaded from environment variables (see `.env.example` in the docker repo).

| Variable                    | Description                                                                        |
| --------------------------- | ---------------------------------------------------------------------------------- |
| `DISCORD_TOKEN`             | Bot token (required)                                                               |
| `DISCORD_APPLICATION_ID`    | Application ID (required)                                                          |
| `DATABASE_HOST`             | PostgreSQL host (default `localhost`)                                              |
| `DATABASE_PORT`             | PostgreSQL port (default `5432`)                                                   |
| `DATABASE_NAME`             | Database name (default `purgebot`)                                                 |
| `DATABASE_USER`             | Database user (default `purgebot`)                                                 |
| `DATABASE_PASSWORD`         | Database password                                                                  |
| `REDIS_ADDR`                | Redis address (default `localhost:6379`)                                           |
| `REDIS_PASSWORD`            | Redis password                                                                     |
| `REDIS_DB`                  | Redis database index (default `0`)                                                 |
| `WORKER_CONCURRENCY`        | Purge workers to run in parallel (default `4`)                                     |
| `PURGE_CHANNEL_CONCURRENCY` | Channels of one purge to delete from at once, `0` for every channel (default `10`) |
| `SENTRY_DSN`                | Sentry-compatible error reporting (optional)                                       |
| `LOG_LEVEL`                 | `debug`, `info`, `warn`, `error` (default `info`)                                  |
| `LOG_JSON`                  | `true` for JSON log output                                                         |
