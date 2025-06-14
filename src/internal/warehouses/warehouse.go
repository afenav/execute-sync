/**
 * Package warehouses provides an abstraction for interacting with different types of databases.
 *
 * The `Database` interface includes the following methods:
 * - `Bootstrap`: Prepares the database for use, such as setting up initial configurations.
 * - `Prune`: Cleans up old or unnecessary data from the database.
 * - `Upload`: Uploads data to the database in chunks, using a callback function to fetch the next record.
 * - `CreateViews`: Creates database views based on the provided schema.
 *
 * The `NewDatabase` function is a factory method that returns a `Database` implementation based on the provided configuration.
 * Currently, it supports the following database types:
 * - "SNOWFLAKE": Returns a Snowflake database implementation.
 *
 * If an unsupported database type is specified, the `New` function returns an error.
 */
package warehouses

import (
	"errors"

	"github.com/afenav/execute-sync/src/internal/config"
	"github.com/afenav/execute-sync/src/internal/execute"
	"github.com/afenav/execute-sync/src/internal/warehouses/snowflake"
	"github.com/afenav/execute-sync/src/internal/warehouses/sqlserver"
	"github.com/afenav/execute-sync/src/internal/warehouses/sqlite"
)

type Database interface {
	Prune() error
	Upload(batch_date string, nextRecord func() (map[string]interface{}, error)) (int, error)
	CreateViews(root execute.RootSchema) error
}

/**
 * NewDatabase creates a new instance of a `Database` implementation based on the provided configuration.
 *
 * Supported Database Types:
 * - "SNOWFLAKE": Returns a Snowflake database implementation.
 * - "SQLITE": Returns a Snowflake database implementation.
 *
 * Parameters:
 * - `cfg` (config.Config): The configuration object
 *
 * Returns:
 * - (Database): A `Database` implementation matching the specified type.
 * - (error): An error if the `DatabaseType` is unsupported or if initialization fails.
 */
func NewDatabase(cfg config.Config) (Database, error) {
	switch cfg.DatabaseType {
	case "SNOWFLAKE":
		return snowflake.NewSnowflake(cfg.DatabaseDSN, cfg.ChunkSize)
	case "SQLSERVER", "MSSQL":
		return sqlserver.NewSQLServer(cfg.DatabaseDSN, cfg.ChunkSize)
	case "GOSQLITE":
		return sqlite.NewSQLite("sqlite", cfg.DatabaseDSN, cfg.ChunkSize)
	case "SQLITE":
		return sqlite.NewSQLite("sqlite3", cfg.DatabaseDSN, cfg.ChunkSize)
	default:
		return nil, errors.New("unsupported database type")
	}
}
