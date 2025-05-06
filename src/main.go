package main

/* =====================================================================
   A helpful go application to pull data from Execute and push it into
   Data Warehouse using Execute's fetch APIs
   ===================================================================== */

import (
	"fmt"
	"os"

	"github.com/AFENav/execute-sync/src/internal/config"
	"github.com/AFENav/execute-sync/src/internal/warehouses"
	"github.com/gofiber/fiber/v2/log"
	"github.com/urfave/cli/v2"
)

var (
	version = "dev"
)

func main() {
	app := &cli.App{
		Usage: "Blast Execute data into a data warehouse",
		Action: func(cCtx *cli.Context) error {
			return cli.ShowAppHelp(cCtx)
		},
		Flags: config.GetFlags(),

		Commands: []*cli.Command{
			ConfigCommand(),
			SyncCommand(),
			PushCommand(),
			CreateViewsCommand(),
			PruneCommand(),
			GenCommand(),
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
	if err != nil {
		log.Errorf("Failed to bootstrap database: %v", err)
		return err
	}
	return action(db, cfg)
}
