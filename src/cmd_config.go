package main

import (
	"fmt"

	"github.com/afenav/execute-sync/src/internal/config"
	"github.com/urfave/cli/v2"
)

func ConfigCommand() *cli.Command {
	return &cli.Command{
		Name:        "config",
		Aliases:     []string{"c"},
		Usage:       "Display configuration",
		Description: "Display the configuration parameters",
		Action: func(cCtx *cli.Context) error {
			cfg := config.ResolveConfig(cCtx)
			fmt.Printf("======== Configuration ========\n")
			fmt.Printf("Warehouse Type     : %s\n", cfg.DatabaseType)
			fmt.Printf("Execute URL        : %s\n", cfg.ExecuteURL)
			fmt.Printf("API Key ID         : %s\n", cfg.ExecuteKeyId)
			fmt.Printf("Include Calcs      : %t\n", cfg.IncludeCalcs)
			fmt.Printf("Max Documents      : %d\n", cfg.MaxDocuments)
			fmt.Printf("Chunk Size         : %d\n", cfg.ChunkSize)
			fmt.Printf("State Directory    : %s\n", cfg.StateDir)
			fmt.Printf("Quiet Logging?     : %t\n", cfg.Quiet)
			fmt.Printf("Wait (for sync)    : %d seconds\n", cfg.Wait)
			return nil
		},
	}
}
