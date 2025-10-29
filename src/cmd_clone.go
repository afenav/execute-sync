package main

import (
	"github.com/afenav/execute-sync/src/internal/config"
	"github.com/afenav/execute-sync/src/internal/execute"
	"github.com/afenav/execute-sync/src/internal/warehouses"
	"github.com/charmbracelet/log"
	"github.com/urfave/cli/v2"
)

func CloneCommand() *cli.Command {
	return &cli.Command{
		Name:        "clone",
		Usage:       "Clone",
		Description: "Combined Create Views and Full Sync",
		Action: func(cCtx *cli.Context) error {
			return withDatabase(cCtx, func(db warehouses.Database, cfg config.Config) error {

				views, err := execute.FetchSchema(cfg)
				if err != nil {
					return err
				}
				err = db.CreateViews(views)
				if err != nil {
					return err
				}
				log.Info("Views Created")

				// Force a complete sync
				cfg.Force = true
				err = sync(cfg, db, true)
				if err != nil {
					return err
				}
				log.Info("Sync Completed")

				return nil
			})
		},
	}
}
