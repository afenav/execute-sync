# Execute Sync

[![Build Status](https://github.com/afenav/execute-sync/actions/workflows/release.yml/badge.svg)](https://github.com/afenav/execute-sync/actions)
[![Go Report Card](https://goreportcard.com/badge/github.com/afenav/execute-sync)](https://goreportcard.com/report/github.com/afenav/execute-sync)
[![Go Reference](https://pkg.go.dev/badge/github.com/afenav/execute-sync.svg)](https://pkg.go.dev/github.com/afenav/execute-sync)

**Execute Sync** is a helpful application designed to pull data from the Execute API and efficiently push it into a data warehouse (Snowflake, Azure SQL, and SQLite) for reporting, BI and warehousing purposes.

[![asciicast](https://asciinema.org/a/lik5zrveLpuOg3kVjd71Igd2u.svg)](https://asciinema.org/a/lik5zrveLpuOg3kVjd71Igd2u)

Complete documentation can be found on the [wiki](https://github.com/afenav/execute-sync/wiki).

## Usage

Running `execute-sync` without arguments or with `--help` will show usage help.  Additionally, you can see command specific documentation with `execute-sync COMMAND --help` (i.e. `execute-sync sync --help` will show arguments specific to the sync function).

Typically, configure Execute-Sync with a `.env` file:

```
EXECUTESYNC_DATABASE_TYPE=SNOWFLAKE
EXECUTESYNC_DATABASE_DSN=execute@???-???/execute/public?warehouse=execute_wh&authenticator=SNOWFLAKE_JWT&privateKey=MIIEvQI

#EXECUTESYNC_DATABASE_TYPE=MSSQL
#EXECUTESYNC_DATABASE_DSN=sqlserver://username:password@localhost:1433?database=MyDatabase

EXECUTESYNC_EXECUTE_URL=https://executedemo.quorumsoftware.com
EXECUTESYNC_EXECUTE_APIKEY_ID=...
EXECUTESYNC_EXECUTE_APIKEY_SECRET=...
```

And then run a full clone to push across all data:

```
execute-sync clone
```

Then periodically sync updates from Execute into the warehouse with:

```
# one-time push.  need to schedule externally
execute-sync push

# periodic push.  execute-sync is long-running
execute-sync sync
```

If the Execute schema changes (upgrade or new fields), update the helper views to match with:

```
execute-sync create_views
```

It also runs great in Docker!
```
# create a volume to store sync state
docker volume create execute_sync

# force a full clone (initial sync + creating helper views)
docker run --rm -it --env-file .env -v execute_sync:/var/run/execute-sync ghcr.io/afenav/execute-sync clone

# start background container for periodically push updates
docker run -d --env-file .env -v execute_sync:/var/run/execute-sync ghcr.io/afenav/execute-sync 
```
