package main

import (
	"fmt"
	"reflect"

	"github.com/afenav/execute-sync/src/internal/config"
	"github.com/charmbracelet/log"
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
			cfgVal := reflect.ValueOf(cfg)
			cfgType := cfgVal.Type()
			for i := 0; i < cfgVal.NumField(); i++ {
				field := cfgType.Field(i)
				name := field.Name
				value := cfgVal.Field(i).Interface()
				// Mask secrets
				if name == "ExecuteKeySecret" || name == "DatabaseDSN" {
					value = "***REDACTED***"
				}
				fmt.Printf("%-18s: %v\n", name, value)
			}
			// Show runtime log info
			fmt.Printf("%-18s: %s\n", "Log Level (min)", log.GetLevel().String())
			return nil
		},
	}
}
