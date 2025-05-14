package main

import (
	"github.com/afenav/execute-sync/src/internal/config"
	"github.com/afenav/execute-sync/src/internal/execute"
	"github.com/afenav/execute-sync/src/internal/warehouses"
	"github.com/urfave/cli/v2"
)

func CreateViewsCommand() *cli.Command {
	return &cli.Command{
		Name:        "create_views",
		Usage:       "Create helper views",
		Description: "Create helper views which make querying data much easier",
		Action: func(cCtx *cli.Context) error {
			return withDatabase(cCtx, func(db warehouses.Database, cfg config.Config) error {
				views, err := execute.FetchSchema(cfg)
				if err != nil {
					return err
				}
				return db.CreateViews(views)
			})
		},
	}
}
