# Execute Sync

[![Build Status](https://github.com/afenav/execute-sync/actions/workflows/release.yml/badge.svg)](https://github.com/afenav/execute-sync/actions)
[![Go Report Card](https://goreportcard.com/badge/github.com/afenav/execute-sync)](https://goreportcard.com/report/github.com/afenav/execute-sync)
[![Go Reference](https://pkg.go.dev/badge/github.com/afenav/execute-sync.svg)](https://pkg.go.dev/github.com/afenav/execute-sync)

**Execute Sync** is a helpful application designed to pull data from the Execute API and push it into a data warehouse (Snowflake, and SQLite) using Execute's `fetch` APIs. 

[![asciicast](https://asciinema.org/a/lik5zrveLpuOg3kVjd71Igd2u.svg)](https://asciinema.org/a/lik5zrveLpuOg3kVjd71Igd2u)

## Basic Configuration

At a minimum, Execute Sync requires:
1. Target database type & credentials 
2. Execute credentials (URL, API Key ID, API Key Secret)

These can be set as environment variables, added to a `.env` file, or provided as command-line arguments.

```
EXECUTESYNC_DATABASE_TYPE=SNOWFLAKE
EXECUTESYNC_DATABASE_DSN=execute@???-???/execute/public?warehouse=execute_wh&authenticator=SNOWFLAKE_JWT&privateKey=MIIEvQI
EXECUTESYNC_EXECUTE_URL=https://executedemo.quorumsoftware.com
EXECUTESYNC_EXECUTE_APIKEY_ID=...
EXECUTESYNC_EXECUTE_APIKEY_SECRET=...
```

```
EXECUTESYNC_DATABASE_TYPE=GOSQLITE # or SQLITE for native driver (requires CGo)
EXECUTESYNC_DATABASE_DSN=./execute.sqlite
EXECUTESYNC_EXECUTE_URL=https://executedemo.quorumsoftware.com
EXECUTESYNC_EXECUTE_APIKEY_ID=...
EXECUTESYNC_EXECUTE_APIKEY_SECRET=...
```

```
# Example: SQL Server (using SQL Server Authentication)
EXECUTESYNC_DATABASE_TYPE=MSSQL
EXECUTESYNC_DATABASE_DSN=sqlserver://username:password@localhost:1433?database=MyDatabase
EXECUTESYNC_EXECUTE_URL=https://executedemo.quorumsoftware.com
EXECUTESYNC_EXECUTE_APIKEY_ID=...
EXECUTESYNC_EXECUTE_APIKEY_SECRET=...
```

```
# Example: SQL Server (using Windows Authentication)
EXECUTESYNC_DATABASE_TYPE=MSSQL
EXECUTESYNC_DATABASE_DSN=sqlserver://localhost:1433?database=MyDatabase&trusted_connection=yes
EXECUTESYNC_EXECUTE_URL=https://executedemo.quorumsoftware.com
EXECUTESYNC_EXECUTE_APIKEY_ID=...
EXECUTESYNC_EXECUTE_APIKEY_SECRET=...
```

Note: SQL Server with a self-signed certificate will fail.  You can work around this by setting the following environment variable:

```
GODEBUG=x509negativeserial=1
```

Additional variables can also be set to control sync wait times, batch sizes, etc.

```
EXECUTESYNC_WAIT=60
```

Execute Sync supports two primary commands:
* `execute-sync sync` - will periodically pull updates from Execute and push them into Snowflake.  
* `execute sync push` - will pull updates from Execute and push them into Snowflake and exit.  This mode is primarily used with external scheduling tools such as CRON.
* By default, both commands store the last sync date in `last_sync_date.txt`.  This date is used to avoid pulling the same records more than once.
* Both commands support `-f` argument which will perform a full read/replace of all data (i.e. `execute sync push -f`).  This is useful for resynchronizing changes due to backend database updates, or to fetch current calculated field values (when using `--calcs`).

Additionally:
* `execute-sync create_views` will create/update helper views that make querying the Snowflake data easier
* `execute-sync prune` should be run periodically to clean up temporary storage.

### Snowflake DSNs

With Snowflake no longer supporting password-based authentication, the preferred mechanism for Execute Sync is `snowflake-jwt`.  Generating and formatting a certificate is tricky, so `execute-sync gen` is an easy option for creating the certificate in the required formats.

The DSN should be in the format:

```
{USER}@{HOST}/{DATABASE}/{SCHEMA}?warehouse={WAREHOUSE}&authenticator=SNOWFLAKE_JWT&privateKey={PRIVATE_KEY}
```

1. `{USER}` is the name of the Snowflake user.  This user must have full permissions to the Database/Schema.  It will need permission to create/update views, tables, stages, formats and pipes, as well as upload files to the stage.   It will also need to have the public key associated with it.
2. `{HOST}` is usually of the format `ORGANIZATION_NAME-ACCOUNT_NAME` (i.e. `123-ABC` or `123-abc.snowflakecomputing.com`)
3. `{DATABASE}` and `{SCHEMA}` is the EMPTY target database that Execute Sync should write to.  The `{USER}` will permissions to manage this database/schema.
2. `{PRIVATE_KEY}` is the PEM private key without the `----BEGIN...` header/footer and without line-breaks.

A starting point for a basic setup could look like the following:

```sql
CREATE DATABASE EXECUTE_DEV;

CREATE ROLE EXECUTE_DEV_API;

CREATE WAREHOUSE EXECUTE_DEV_WH
WAREHOUSE_SIZE = 'XSMALL'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE
INITIALLY_SUSPENDED = TRUE;

GRANT USAGE ON WAREHOUSE EXECUTE_DEV_WH TO ROLE EXECUTE_DEV_API;
GRANT ALL PRIVILEGES ON DATABASE EXECUTE_DEV TO ROLE EXECUTE_DEV_API;
GRANT USAGE ON ALL SCHEMAS IN DATABASE EXECUTE_DEV TO ROLE EXECUTE_DEV_API;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE EXECUTE_DEV TO ROLE EXECUTE_DEV_API;
GRANT ALL PRIVILEGES ON FUTURE SCHEMAS IN DATABASE EXECUTE_DEV TO ROLE EXECUTE_DEV_API;
GRANT ALL PRIVILEGES ON ALL TABLES IN DATABASE EXECUTE_DEV TO ROLE EXECUTE_DEV_API;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN DATABASE EXECUTE_DEV TO ROLE EXECUTE_DEV_API;

CREATE USER EXECUTE_DEV
TYPE = service
RSA_PUBLIC_KEY='-----BEGIN PUBLIC KEY-----
....
-----END PUBLIC KEY-----'
DEFAULT_ROLE = 'EXECUTE_DEV_API'
DEFAULT_WAREHOUSE = 'EXECUTE_DEV_WH';

GRANT ROLE EXECUTE_DEV_API TO USER EXECUTE_DEV;
```

## Usage

Running `execute-sync` without arguments or with `--help` will show usage help.  Additionally, you can see command specific documentation with `execute-sync COMMAND --help` (i.e. `execute-sync sync --help` will show arguments specific to the sync function).

## Docker Usage

Assuming you've defined the necessary variables in a local `.env` file, you can easily start Execute Sync in sync mode where it will periodically, and efficiently, push changes from Execute into the target database.

**NOTE that the sync high-water mark is stored in `/var/run/execute-sync`, and it's usually a good idea to mount a volume to that location to preserve state across runs.**

```
docker run --rm -it --env-file .env ghcr.io/afenav/execute-sync 

# or, better yet, with a bind mount to save sync state between runs
mkdir -p state && chown 6001:6001
docker run --rm -it --env-file .env -v ./state:/var/run/execute-sync ghcr.io/afenav/execute-sync 
```

Similarly, you could do the same with Docker Compose.

```docker
services:
  sync:
    image: ghcr.io/afenav/execute-sync:${TAG:-latest}
    environment:
      - EXECUTESYNC_DATABASE_TYPE=${EXECUTESYNC_DATABASE_TYPE}
      - EXECUTESYNC_DATABASE_DSN=${EXECUTESYNC_DATABASE_DSN}
      - EXECUTESYNC_EXECUTE_URL=${EXECUTESYNC_EXECUTE_URL}
      - EXECUTESYNC_EXECUTE_APIKEY_ID=${EXECUTESYNC_EXECUTE_APIKEY_ID}
      - EXECUTESYNC_EXECUTE_APIKEY_SECRET=${EXECUTESYNC_EXECUTE_APIKEY_SECRET}
      - EXECUTESYNC_WAIT=${EXECUTESYNC_WAIT:-600}
    env_file:
      - .env
    volumes:
      - syncstate:/var/run/execute-sync
    restart: unless-stopped
volumes:
  syncstate:
```

To run a different command than the default `sync`...

```
docker run --rm -it --env-file .env ghcr.io/afenav/execute-sync push -f
```

## Development

### Go Version
This project tracks the latest GoLang release.  If there is ever a need to pin it to a specific version...

* Development environments / DevContainers are defined in `mise.toml`
* Releases are defined by `.github/workflows/release.yml` 

### Upgrading Dev Dependencies
All project dependencies can be upgraded using the following:

```
mise run upgrade-deps
```

Note: The sqlite library has a fragile relationship to the libc library and requires separate/manual updating.

### Testing Upgrades

```
go build -ldflags "-X main.version=0.1.10" -o execute-sync ./src && ./execute-sync upgrade
```

### Releases
Releases builds (binaries and docker) automatically trigger when new tags are pushed to main.  Release notes are automatically created based on commit messages.

```
VERSION=0.1.20 mise run release
```
