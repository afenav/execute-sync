package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strconv"

	"github.com/charmbracelet/log"
	"github.com/goloop/env"
	"github.com/urfave/cli/v2"
)

type Config struct {
	ExecuteURL       string `env:"EXECUTE_URL" flag:"execute-url" usage:"The Execute API URL" alias:"u" required:"true"`
	ExecuteKeyId     string `env:"EXECUTE_APIKEY_ID" flag:"execute-key-id" usage:"The Execute API Key ID" required:"true"`
	ExecuteKeySecret string `env:"EXECUTE_APIKEY_SECRET" flag:"execute-key-secret" usage:"The Execute API Key Secret" required:"true"`
	MaxDocuments     int    `env:"MAX_DOCUMENTS" def:"1000" flag:"max-documents" usage:"Maximum number of documents to fetch" alias:"m" default:"10000"`
	DatabaseType     string `env:"DATABASE_TYPE" flag:"database-type" usage:"Type of database connection" required:"true"`
	DatabaseDSN      string `env:"DATABASE_DSN" flag:"database-dsn" usage:"DSN for database connection" required:"true"`
	StateDir         string `env:"STATE_DIR" def:"." flag:"state-dir" usage:"Directory to store state files" alias:"d" default:"."`
	Wait             int    `env:"WAIT" def:"600" flag:"wait" usage:"Wait time in seconds" default:"600"`
	ChunkSize        int    `env:"CHUNK_SIZE" def:"10000" flag:"chunk-size" usage:"Chunk size for processing large data" alias:"c" default:"10000"`
	IncludeCalcs     bool   `env:"INCLUDE_CALCS" def:"false" flag:"include-calcs" usage:"Include calculated values in fetch" alias:"x" default:"false"`
	LogLevel         string `env:"LOG_LEVEL" flag:"log-level" usage:"Log level: quiet, info, debug" alias:"l" default:"info"`
	Force            bool   `env:"FORCE" def:"false" flag:"force" usage:"Force operation" default:"false"`
}

// GetFlags returns the CLI flags for the application, centralized here for consistency
func GetFlags() []cli.Flag {
	cfgType := reflect.TypeOf(Config{})
	var flags []cli.Flag
	for i := 0; i < cfgType.NumField(); i++ {
		field := cfgType.Field(i)
		flagName := field.Tag.Get("flag")
		usage := field.Tag.Get("usage")
		alias := field.Tag.Get("alias")
		def := field.Tag.Get("default")

		if flagName == "" {
			continue
		}

		aliases := []string{}
		if alias != "" {
			aliases = append(aliases, alias)
		}

		// Derive envvar as EXECUTE_{ENV}
		envTag := field.Tag.Get("env")
		envvars := []string{}
		if envTag != "" {
			envvars = append(envvars, "EXECUTESYNC_"+envTag)
		}

		switch field.Type.Kind() {
		case reflect.String:
			flags = append(flags, &cli.StringFlag{
				Name:        flagName,
				Usage:       usage,
				EnvVars:     envvars,
				Aliases:     aliases,
				DefaultText: def,
			})
		case reflect.Int:
			defVal, _ := strconv.Atoi(def)
			flags = append(flags, &cli.IntFlag{
				Name:    flagName,
				Usage:   usage,
				EnvVars: envvars,
				Aliases: aliases,
				Value:   defVal,
			})
		case reflect.Bool:
			defVal, _ := strconv.ParseBool(def)
			flags = append(flags, &cli.BoolFlag{
				Name:    flagName,
				Usage:   usage,
				EnvVars: envvars,
				Aliases: aliases,
				Value:   defVal,
			})
		}
	}
	return flags
}

func ResolveConfig(cCtx *cli.Context) Config {
	var cfg Config
	cfgVal := reflect.ValueOf(&cfg).Elem()
	cfgType := cfgVal.Type()

	// Parse the configuration (environment, with .env override)
	if fileExists(".env") {
		if err := env.Load(".env"); err != nil {
			log.Fatal(err)
		}
	}

	if err := env.Unmarshal("EXECUTESYNC_", &cfg); err != nil {
		log.Fatal(err)
	}

	for i := 0; i < cfgType.NumField(); i++ {
		field := cfgType.Field(i)
		flagName := field.Tag.Get("flag")
		if flagName == "" {
			continue
		}
		if !cCtx.IsSet(flagName) {
			continue
		}
		val := cfgVal.Field(i)
		switch field.Type.Kind() {
		case reflect.String:
			val.SetString(cCtx.String(flagName))
		case reflect.Int:
			val.SetInt(int64(cCtx.Int(flagName)))
		case reflect.Bool:
			val.SetBool(cCtx.Bool(flagName))
		}
	}

	// Special case for SQLITE.  If a DSN isn't provided, default to storing the DB in the state
	// directory.  This plays nicely with Dockerized environments.
	if (cfg.DatabaseType == "SQLITE" || cfg.DatabaseType == "GOSQLITE") && cfg.DatabaseDSN == "" {
		cfg.DatabaseDSN = filepath.Join(cfg.StateDir, "execute.sqlite")
	}

	errors := false
	for i := 0; i < cfgType.NumField(); i++ {
		field := cfgType.Field(i)
		required := field.Tag.Get("required") == "true"
		if !required {
			continue
		}
		val := cfgVal.Field(i)
		empty := false
		switch field.Type.Kind() {
		case reflect.String:
			empty = val.String() == ""
		case reflect.Int:
			empty = val.Int() == 0
		case reflect.Bool:
			// bools are never required
		}
		if empty {
			log.Warnf("%s is required", field.Tag.Get("env"))
			errors = true
		}
	}

	if errors {
		os.Exit(1)
	}

	return cfg
}

func fileExists(filePath string) bool {
	_, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		return false
	}
	return err == nil
}
