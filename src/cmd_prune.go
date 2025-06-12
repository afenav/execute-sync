package main

import (
	"github.com/afenav/execute-sync/src/internal/config"
	"github.com/afenav/execute-sync/src/internal/warehouses"
	"github.com/charmbracelet/log"
	"github.com/urfave/cli/v2"
)

func PruneCommand() *cli.Command {
	return &cli.Command{
		Name:        "prune",
		Usage:       "Prune unused data",
		Description: "Prune unused/temporary data from warehouse",
		Action: func(cCtx *cli.Context) error {
			return withDatabase(cCtx, func(db warehouses.Database, cfg config.Config) error {
				if err := db.Prune(); err != nil {
					return err
				}

				log.Info("Pruning Completed!")
				return nil
			})
		},
	}
}
