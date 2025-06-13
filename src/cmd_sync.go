package main

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/afenav/execute-sync/src/internal/config"
	"github.com/afenav/execute-sync/src/internal/warehouses"
	"github.com/charmbracelet/log"
	"github.com/urfave/cli/v2"
)

func SyncCommand() *cli.Command {
	return &cli.Command{
		Name:    "sync",
		Aliases: []string{"s"},
		Flags: []cli.Flag{
			&cli.IntFlag{Name: "wait", Usage: "Wait time in seconds between sync iterations", EnvVars: []string{"EXECUTESYNC_WAIT"}, DefaultText: "600", Aliases: []string{"w"}},
		},
		Usage:       "Periodically sync new updates to warehouse",
		Description: "Sync new updates based on the configured WAIT",
		Action: func(cCtx *cli.Context) error {
			return withDatabase(cCtx, func(db warehouses.Database, cfg config.Config) error {
				return sync(cfg, db, false)
			})
		},
	}
}

func PushCommand() *cli.Command {
	return &cli.Command{
		Name:    "push",
		Aliases: []string{"p"},
		Flags: []cli.Flag{
			&cli.BoolFlag{Name: "force", Usage: "Force a complete data refresh", EnvVars: []string{"EXECUTESYNC_FORCE"}, DefaultText: "false", Aliases: []string{"f"}},
		},
		Usage:       "Onetime push of new updates to warehouse",
		Description: "Pushes a set of updates to warehouse and terminates",
		Action: func(cCtx *cli.Context) error {
			return withDatabase(cCtx, func(db warehouses.Database, cfg config.Config) error {
				return sync(cfg, db, true)
			})
		},
	}
}

func sync(cfg config.Config, db warehouses.Database, onetime bool) error {

	for {
		log.Info("Starting Sync")
		count, err := fetchAndProcessDocuments(cfg, db)
		if err != nil {
			log.Infof("Sync Failed: %v", err)
		} else if count == 0 {
			log.Info("Sync Complete: No Updated Documents")
		} else {
			log.Infof("Sync Complete: %d Updated Documents", count)
		}
		if cfg.Wait == 0 || onetime {
			break
		}
		log.Infof("Sleeping %d seconds", cfg.Wait)
		time.Sleep(time.Duration(cfg.Wait) * time.Second)
	}
	return nil
}

func fetchAndProcessDocuments(cfg config.Config, db warehouses.Database) (int, error) {

	batch_date := time.Now().UTC().Format("2006-01-02T15:04:05Z")

	// Keep track of document count
	document_count := 0

	// Fetch the data of the last successful sync
	lastSyncDate := loadLastSyncDate(cfg.StateDir)

	// If we have no last sync date, or we're forcing a full refresh, pick a date way in the past
	if cfg.Force || lastSyncDate == "" {
		lastSyncDate = "1900-01-01"
	}

	// Depending on the number of documents and batch sizes, we may have to perform several iterations before
	// We can slurp down all the documents
	for {

		// Perform the GET request
		client := &http.Client{}

		// Parse the base URL
		parsedURL, err := url.Parse(cfg.ExecuteURL)
		if err != nil {
			return 0, fmt.Errorf("parsing execute URL: %v", err)
		}

		// Appends the Fetch API to the BASE URI
		parsedURL = parsedURL.JoinPath("/fetch/document/")

		// Add query string parameters to the URL
		query := parsedURL.Query()
		query.Set("limit", fmt.Sprint(cfg.MaxDocuments))
		query.Set("since", lastSyncDate)
		if cfg.IncludeCalcs {
			query.Set("calc", "true")
		}
		parsedURL.RawQuery = query.Encode()

		// Fetch the data
		req, err := http.NewRequest("GET", parsedURL.String(), nil)
		if err != nil {
			return 0, fmt.Errorf("creating request: %v", err)
		}

		// Add credentials to the request (Execute uses BASIC Auth)
		auth := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", cfg.ExecuteKeyId, cfg.ExecuteKeySecret)))
		req.Header.Set("Authorization", "Basic "+auth)

		log.Debug("Pulling batch from Execute")
		resp, err := client.Do(req)
		if err != nil {
			return 0, fmt.Errorf("performing request: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return 0, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}

		reader := bufio.NewReader(resp.Body)

		// Helper function to read the next record from the reader.  Records
		// are newline delimited
		nextRecord := func() (map[string]interface{}, error) {
			line, err := reader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					return nil, io.EOF
				}
				return nil, err
			}

			var record map[string]interface{}
			if err := json.Unmarshal([]byte(line), &record); err != nil {
				log.Infof("Error parsing JSON: %v", err)
				return nil, nil
			}
			return record, nil
		}

		// Upload all documents in this batch.  Note that we're passing in a
		// reader callback so that we're not assembling all these documents in
		// memory since this can easily become very large.
		log.Debug("Uploading batch to warehouse")
		cnt, err := db.Upload(batch_date, nextRecord)
		if err != nil {
			return 0, err
		}

		// Increase our global document count
		document_count += cnt

		// Assuming we made it this far, lets store the returned sync highwater
		// mark so that we can avoid these records on future syncs
		lastSyncDate = resp.Header.Get("X-Sync-Highwater-Mark")
		log.Debugf("Storing last sync date = %s", lastSyncDate)
		saveLastSyncDate(cfg.StateDir, lastSyncDate)

		// If we the result set we pulled is complete, we can break and avoid further iterations
		if strings.ToUpper(resp.Header.Get("X-Sync-Truncated")) == "FALSE" {
			break
		}
	}

	// Return the number of documents successfully processed
	return document_count, nil
}

func loadLastSyncDate(basePath string) string {
	filePath := filepath.Join(basePath, "last_sync_date.txt")
	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return ""
		}
		log.Fatalf("Error reading last sync date: %v", err)
	}
	return strings.TrimSpace(string(data))
}

func saveLastSyncDate(basePath string, date string) {
	filePath := filepath.Join(basePath, "last_sync_date.txt")
	if err := os.WriteFile(filePath, []byte(date), 0644); err != nil {
		log.Fatalf("Error saving last sync date: %v", err)
	}
}
