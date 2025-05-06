package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/AFENav/execute-sync/src/internal/config"
	"github.com/AFENav/execute-sync/src/internal/execute"
	"github.com/AFENav/execute-sync/src/internal/warehouses"
	"github.com/gofiber/fiber/v2/log"
	"github.com/urfave/cli/v2"
)

func CreateViewsCommand() *cli.Command {
	return &cli.Command{
		Name:        "create_views",
		Usage:       "Create helper views",
		Description: "Create helper views which make querying data much easier",
		Action: func(cCtx *cli.Context) error {
			return withDatabase(cCtx, func(db warehouses.Database, cfg config.Config) error {
				client := &http.Client{}

				// Parse the base URL
				parsedURL, err := url.Parse(cfg.ExecuteURL)
				if err != nil {
					return fmt.Errorf("parsing execute URL: %v", err)
				}

				// Appends the Fetch API to the BASE URI
				parsedURL = parsedURL.JoinPath("/fetch/document/schema")

				// Add query string parameters to the URL
				query := parsedURL.Query()
				if cfg.IncludeCalcs {
					query.Set("calc", "true")
				}
				parsedURL.RawQuery = query.Encode()

				// Fetch the data
				req, err := http.NewRequest("GET", parsedURL.String(), nil)
				if err != nil {
					return fmt.Errorf("creating request: %v", err)
				}

				// Add credentials to the request (Execute uses BASIC Auth)
				auth := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", cfg.ExecuteKeyId, cfg.ExecuteKeySecret)))
				req.Header.Set("Authorization", "Basic "+auth)

				log.Debug("Pulling schema from Execute")
				resp, err := client.Do(req)
				if err != nil {
					return fmt.Errorf("performing request: %v", err)
				}
				defer resp.Body.Close()

				if resp.StatusCode != http.StatusOK {
					return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
				}

				bodyBytes, _ := io.ReadAll(resp.Body)

				// Parse the retrieve document as JSON so that we can extract metadata fields
				var data execute.RootSchema
				if err := json.Unmarshal(bodyBytes, &data); err != nil {
					return fmt.Errorf("Error parsing schema: %s\n", err)
				}

				return db.CreateViews(data)
			})
		},
	}
}
