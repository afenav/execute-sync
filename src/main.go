package main

/* =====================================================================
   A helpful go application to pull data from Execute and push it into
   Data Warehouse using Execute's fetch APIs
   ===================================================================== */

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/afenav/execute-sync/src/internal/config"
	"github.com/afenav/execute-sync/src/internal/warehouses"
	"github.com/charmbracelet/log"
	"github.com/urfave/cli/v2"
)

var (
	version = "dev"
)

// checkLatestVersion checks the latest GitHub release and logs a warning if not running the latest version
func checkLatestVersion() {
	// Skip version check if running in dev mode
	if version == "dev" {
		return
	}

	// Make request to GitHub API
	resp, err := http.Get("https://api.github.com/repos/afenav/execute-sync/releases/latest")
	if err != nil {
		log.Debug("Failed to check for latest version: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		log.Debugf("Failed to check for latest version: GitHub API returned status %d, Body: %s", resp.StatusCode, string(body))
		return
	}

	var release struct {
		TagName string `json:"tag_name"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&release); err != nil {
		log.Debug("Failed to parse GitHub response: %v", err)
		return
	}

	// Clean the version string (remove 'v' prefix if present)
	latestVersion := strings.TrimPrefix(release.TagName, "v")
	currentVersion := strings.TrimPrefix(version, "v")

	if latestVersion != currentVersion {
		log.Warn("Update available!", "current", version, "latest", latestVersion)
	}
}

func main() {

	app := &cli.App{
		Usage: "Blast Execute data into a data warehouse",
		Action: func(cCtx *cli.Context) error {
			return cli.ShowAppHelp(cCtx)
		},
		Flags: config.GetFlags(),
		Before: func(cCtx *cli.Context) error {
			cfg := config.ResolveConfig(cCtx)
			logLevel := log.InfoLevel
			logCaller := false
			switch strings.ToLower(cfg.LogLevel) {
			case "quiet":
				logLevel = log.WarnLevel
				logCaller = false
			case "debug":
				logLevel = log.DebugLevel
				logCaller = true
			default:
			}

			var logger *log.Logger
			var logFile *os.File
			if cfg.LogFile != "" {
				var err error
				logFile, err = os.OpenFile(cfg.LogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to open log file %s: %v\n", cfg.LogFile, err)
					logger = log.NewWithOptions(os.Stderr, log.Options{
						ReportCaller:    logCaller,
						ReportTimestamp: true,
						Level:           logLevel,
					})
				} else {
					multi := io.MultiWriter(os.Stderr, logFile)
					logger = log.NewWithOptions(multi, log.Options{
						ReportCaller:    logCaller,
						ReportTimestamp: true,
						Level:           logLevel,
					})
					// Store logFile in context for After hook
					cCtx.App.Metadata = map[string]interface{}{"logFile": logFile}
				}
			} else {
				logger = log.NewWithOptions(os.Stderr, log.Options{
					ReportCaller:    logCaller,
					ReportTimestamp: true,
					Level:           logLevel,
				})
			}

			log.SetDefault(logger)
			checkLatestVersion()
			return nil
		},
		After: func(cCtx *cli.Context) error {
			if lf, ok := cCtx.App.Metadata["logFile"]; ok {
				if logFile, ok := lf.(*os.File); ok {
					logFile.Close()
				}
			}
			return nil
		},
		Commands: []*cli.Command{
			ConfigCommand(),
			SyncCommand(),
			PushCommand(),
			CreateViewsCommand(),
			PruneCommand(),
			CloneCommand(),
			GenCommand(),
			UpgradeCommand(),
			{
				Name:        "version",
				Aliases:     []string{"v"},
				Usage:       "Display Version",
				Description: "Display software version number",
				Action: func(cCtx *cli.Context) error {
					fmt.Println(version)
					return nil
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}

}

// Helper function to resolve configuration and initialize the database
func withDatabase(cCtx *cli.Context, action func(db warehouses.Database, cfg config.Config) error) error {
	cfg := config.ResolveConfig(cCtx)
	db, err := warehouses.NewDatabase(cfg)
	if err != nil {
		log.Errorf("Failed to initialize database: %v", err)
		return err
	}
	return action(db, cfg)
}
