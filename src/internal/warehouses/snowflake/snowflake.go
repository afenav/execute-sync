package snowflake

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/afenav/execute-sync/src/internal/execute"
	"github.com/charmbracelet/log"
	_ "github.com/snowflakedb/gosnowflake"
)

const (
	TableName          string = "EXECUTE_DOCUMENTS"
	DefaultMaxJSONSize int    = 10 * 1024 * 1024 // 10MB - Snowflake VARIANT recommended limit
	WarningJSONSize    int    = 8 * 1024 * 1024  // 8MB - warn at 80% of limit
	ExtremeJSONSize    int    = 15 * 1024 * 1024 // 15MB - fail fast on extremely large objects
)

type Snowflake struct {
	dsn       string
	chunkSize int
}

// UploadStats tracks statistics during the upload process
type UploadStats struct {
	DocumentsProcessed  int
	ChunksWritten       int
	ChunksFailedToWrite int
	LargeJSONWarnings   int
	ExtremeJSONFailures int
	StartTime           time.Time
	CSVWriteErrors      []string
}

// validateJSONSize checks if JSON size is within acceptable limits and logs warnings/errors
func validateJSONSize(jsonBytes []byte, documentID string, chunkIndex int) error {
	size := len(jsonBytes)

	if size >= ExtremeJSONSize {
		log.Error("JSON object exceeds extreme size limit, failing fast",
			"document_id", documentID,
			"chunk_index", chunkIndex,
			"size_bytes", size,
			"size_mb", float64(size)/1024/1024,
			"extreme_limit_mb", float64(ExtremeJSONSize)/1024/1024)
		return fmt.Errorf("JSON object size %d bytes (%.2f MB) exceeds extreme limit of %d bytes (%.2f MB) for document %s chunk %d",
			size, float64(size)/1024/1024, ExtremeJSONSize, float64(ExtremeJSONSize)/1024/1024, documentID, chunkIndex)
	}

	if size >= WarningJSONSize {
		log.Warn("Large JSON object detected, may impact Snowflake performance",
			"document_id", documentID,
			"chunk_index", chunkIndex,
			"size_bytes", size,
			"size_mb", float64(size)/1024/1024,
			"warning_limit_mb", float64(WarningJSONSize)/1024/1024,
			"max_limit_mb", float64(DefaultMaxJSONSize)/1024/1024)
		return nil
	}

	return nil
}

// validateRequiredFields ensures all required fields are present before processing
func validateRequiredFields(data map[string]interface{}) error {
	requiredFields := []string{"$TYPE", "DOCUMENT_ID", "$VERSION", "$AUTHOR_ID", "$DATE", "$DELETED"}

	for _, field := range requiredFields {
		if _, exists := data[field]; !exists {
			return fmt.Errorf("required field '%s' is missing from document", field)
		}
		if data[field] == nil {
			return fmt.Errorf("required field '%s' is null in document", field)
		}
	}

	return nil
}

func NewSnowflake(dsn string, chunkSize int) (*Snowflake, error) {
	return &Snowflake{
		dsn:       dsn,
		chunkSize: chunkSize,
	}, nil
}

func bootstrap(db *sql.DB) error {

	_, err := db.Exec(fmt.Sprintf(`
	create file format if not exists %s_FORMAT TYPE = CSV SKIP_HEADER=1 TRIM_SPACE=true FIELD_OPTIONALLY_ENCLOSED_BY = '"'
	`, TableName))
	if err != nil {
		return fmt.Errorf("Error creating format: %v", err)
	}

	_, err = db.Exec(fmt.Sprintf(`
	create stage if not exists %s_stage file_format = '%s_FORMAT'
	`, TableName, TableName))
	if err != nil {
		return fmt.Errorf("Error creating stage: %v", err)
	}

	_, err = db.Exec(fmt.Sprintf(`
	create table if not exists %s (
		BATCH_DATE TIMESTAMP_NTZ(9) NOT NULL,
		TYPE VARCHAR(50) NOT NULL,
		ID VARCHAR(50) NOT NULL,
		VERSION NUMBER(38,0) NOT NULL,
		CHUNK NUMBER(38,0) NOT NULL,
		AUTHOR VARCHAR(50),
		DATE TIMESTAMP_NTZ(9) NOT NULL,
		DELETED BOOLEAN NOT NULL,
		DATA VARIANT NOT NULL,
		constraint %s_PK primary key (BATCH_DATE, TYPE, ID, VERSION, CHUNK)
	);
	`, TableName, TableName))
	if err != nil {
		return fmt.Errorf("Error creating table: %v", err)
	}

	_, err = db.Exec(fmt.Sprintf(`
	CREATE PIPE if not exists %s_pipe
	AS COPY INTO %s
	FROM @%s_stage
	FILE_FORMAT = '%s_FORMAT'
	`, TableName, TableName, TableName, TableName))
	if err != nil {
		return fmt.Errorf("Error creating stage: %v", err)
	}
	return nil
}

func (s *Snowflake) Prune() error {
	db, err := sql.Open("snowflake", s.dsn)
	if err != nil {
		return fmt.Errorf("Error connecting to database: %v", err)
	}
	if err = bootstrap(db); err != nil {
		return fmt.Errorf("Error bootstrapping database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(fmt.Sprintf(`
	DELETE FROM %s
	WHERE (TYPE, ID, VERSION, BATCH_DATE) NOT IN (
		SELECT TYPE, ID, VERSION, MAX(BATCH_DATE)
		FROM %s
		GROUP BY TYPE, ID, VERSION
	)
	`, TableName, TableName))

	if err != nil {
		return err
	}

	_, err = db.Exec(fmt.Sprintf(`
	REMOVE @%s_STAGE
	`, TableName))
	if err != nil {
		log.Fatalf("Error pruning stage: %v", err)
	}

	if err != nil {
		return err
	}

	return nil
}

func (s *Snowflake) Upload(batch_date string, nextRecord func() (map[string]interface{}, error)) (int, error) {
	startTime := time.Now()
	log.Info("Starting Snowflake upload", "batch_date", batch_date, "chunk_size", s.chunkSize)

	db, err := sql.Open("snowflake", s.dsn)
	if err != nil {
		return 0, fmt.Errorf("Error connecting to database: %v", err)
	}
	if err = bootstrap(db); err != nil {
		return 0, fmt.Errorf("Error bootstrapping database: %v", err)
	}
	defer db.Close()

	// Initialize statistics tracking
	stats := &UploadStats{
		StartTime: startTime,
	}

	tempDir := os.TempDir()

	// Sanitize batch_date to remove ':' and '-'
	safeBatchDate := strings.ReplaceAll(strings.ReplaceAll(batch_date, ":", ""), "-", "")

	tempFile, err := os.CreateTemp(tempDir, fmt.Sprintf("documents_%s*.csv", safeBatchDate))
	if err != nil {
		return 0, fmt.Errorf("Error creating temporary file: %v", err)
	}
	defer func() {
		tempFile.Close()
		os.Remove(tempFile.Name()) // Cleanup the temp file after the upload
	}()

	log.Debug("Created temporary CSV file", "path", tempFile.Name())

	// Create a CSV writer
	csvWriter := csv.NewWriter(tempFile)

	// Write the CSV headers
	headers := []string{"BATCH_DATE", "TYPE", "ID", "VERSION", "CHUNK", "AUTHOR", "DATE", "DELETED", "DATA"}
	if err := csvWriter.Write(headers); err != nil {
		return 0, fmt.Errorf("Error writing CSV headers: %v", err)
	}

	empty_batch := true

	for {
		data, err := nextRecord()

		// Terminate at EOF
		if err != nil {
			if err.Error() == "EOF" {
				break
			} else {
				log.Error("Error reading next record", "error", err)
				// Continue processing other records instead of failing completely
				continue
			}
		}

		// Skip empty records
		if data == nil {
			continue
		}

		// Validate required fields before processing
		if err := validateRequiredFields(data); err != nil {
			log.Error("Skipping record due to validation failure", "error", err, "batch_date", batch_date)
			continue
		}

		log.Debug("Processing document", "document_id", data["DOCUMENT_ID"], "type", data["$TYPE"])

		// Apply chunking
		var chunks []map[string]interface{}

		// Iterate through the top-level keys
		for key, value := range data {
			// Is this a list key?
			if list, ok := value.([]interface{}); ok {
				// Does this list have #items > chunk size?
				if len(list) > s.chunkSize {
					log.Debug("Chunking large list",
						"document_id", data["DOCUMENT_ID"],
						"field", key,
						"list_size", len(list),
						"chunk_size", s.chunkSize)

					for i := 0; i < len(list); i += s.chunkSize {
						end := i + s.chunkSize
						if end > len(list) {
							end = len(list)
						}

						chunks = append(chunks, map[string]interface{}{
							"DOCUMENT_ID": data["DOCUMENT_ID"].(string),
							key:           list[i:end],
						})
					}

					// Remove the large list from the original document
					delete(data, key)
				}
			}
		}

		// Add the modified original document back to the result
		chunks = append([]map[string]interface{}{data}, chunks...)

		log.Debug("Created chunks for document", "document_id", data["DOCUMENT_ID"], "chunk_count", len(chunks))

		for i := 0; i < len(chunks); i++ {
			// Improved JSON marshaling with error handling and size validation
			chunkBytes, err := json.Marshal(chunks[i])
			if err != nil {
				errMsg := fmt.Sprintf("Failed to marshal JSON for document %s chunk %d: %v",
					data["DOCUMENT_ID"].(string), i, err)
				log.Error("JSON marshaling failed",
					"document_id", data["DOCUMENT_ID"],
					"chunk_index", i,
					"error", err,
					"batch_date", batch_date)
				stats.CSVWriteErrors = append(stats.CSVWriteErrors, errMsg)
				continue
			}

			// Validate JSON size and handle large objects
			if err := validateJSONSize(chunkBytes, data["DOCUMENT_ID"].(string), i); err != nil {
				log.Error("JSON size validation failed",
					"document_id", data["DOCUMENT_ID"],
					"chunk_index", i,
					"error", err,
					"batch_date", batch_date)
				stats.ExtremeJSONFailures++
				stats.CSVWriteErrors = append(stats.CSVWriteErrors, err.Error())
				continue
			}

			// Track large JSON warnings
			if len(chunkBytes) >= WarningJSONSize {
				stats.LargeJSONWarnings++
			}

			// Convert to a CSV row
			csvRecord := []string{
				batch_date,
				data["$TYPE"].(string),
				data["DOCUMENT_ID"].(string),
				fmt.Sprintf("%d", int(data["$VERSION"].(float64))),
				fmt.Sprintf("%d", i),
				data["$AUTHOR_ID"].(string),
				data["$DATE"].(string),
				fmt.Sprintf("%t", data["$DELETED"].(bool)),
				string(chunkBytes),
			}

			// Enhanced CSV error handling with detailed tracking
			if err := csvWriter.Write(csvRecord); err != nil {
				errMsg := fmt.Sprintf("Failed to write CSV record for document %s chunk %d: %v",
					data["DOCUMENT_ID"].(string), i, err)
				log.Error("CSV write failed",
					"document_id", data["DOCUMENT_ID"],
					"chunk_index", i,
					"error", err,
					"batch_date", batch_date)
				stats.ChunksFailedToWrite++
				stats.CSVWriteErrors = append(stats.CSVWriteErrors, errMsg)
				continue
			}

			stats.ChunksWritten++
		}

		// Keep track of the number of documents processed in this run
		stats.DocumentsProcessed++
		empty_batch = false

		// Log progress periodically for large batches
		if stats.DocumentsProcessed%1000 == 0 {
			elapsed := time.Since(startTime)
			log.Info("Upload progress",
				"documents_processed", stats.DocumentsProcessed,
				"chunks_written", stats.ChunksWritten,
				"chunks_failed", stats.ChunksFailedToWrite,
				"elapsed_seconds", int(elapsed.Seconds()),
				"batch_date", batch_date)
		}
	}

	// Flush any remaining data to the CSV file
	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return 0, fmt.Errorf("Error finalizing CSV file: %v", err)
	}

	// Log detailed statistics
	elapsed := time.Since(startTime)
	log.Info("Upload processing completed",
		"batch_date", batch_date,
		"documents_processed", stats.DocumentsProcessed,
		"chunks_written", stats.ChunksWritten,
		"chunks_failed_to_write", stats.ChunksFailedToWrite,
		"large_json_warnings", stats.LargeJSONWarnings,
		"extreme_json_failures", stats.ExtremeJSONFailures,
		"csv_errors_count", len(stats.CSVWriteErrors),
		"processing_time_seconds", int(elapsed.Seconds()),
		"empty_batch", empty_batch)

	// Log CSV errors if any occurred
	if len(stats.CSVWriteErrors) > 0 {
		log.Warn("CSV write errors occurred during upload",
			"error_count", len(stats.CSVWriteErrors),
			"batch_date", batch_date)
		for i, errMsg := range stats.CSVWriteErrors {
			if i < 10 { // Limit to first 10 errors to avoid log spam
				log.Debug("CSV write error detail", "error", errMsg, "batch_date", batch_date)
			}
		}
		if len(stats.CSVWriteErrors) > 10 {
			log.Debug("Additional CSV errors truncated",
				"additional_errors", len(stats.CSVWriteErrors)-10,
				"batch_date", batch_date)
		}
	}

	// Don't push an empty batch to Snowflake
	if empty_batch {
		log.Info("Skipping empty batch upload to Snowflake", "batch_date", batch_date)
		return stats.DocumentsProcessed, nil
	}

	// Enhanced Snowflake pipeline monitoring
	stageStartTime := time.Now()

	// Upload the temporary CSV file to the Snowflake stage
	log.Info("Uploading CSV to Snowflake Stage",
		"file_path", tempFile.Name(),
		"batch_date", batch_date,
		"documents_count", stats.DocumentsProcessed,
		"chunks_count", stats.ChunksWritten)

	putCommand := fmt.Sprintf("PUT '%s' @%s_stage", pathToFileURL(tempFile.Name()), TableName)
	result, err := db.Exec(putCommand)
	if err != nil {
		return 0, fmt.Errorf("Error uploading file to Snowflake stage (batch: %s): %v", batch_date, err)
	}

	stageElapsed := time.Since(stageStartTime)
	log.Info("File upload to stage completed",
		"batch_date", batch_date,
		"upload_time_seconds", int(stageElapsed.Seconds()))

	// Log result details if available
	if result != nil {
		if rowsAffected, err := result.RowsAffected(); err == nil {
			log.Debug("Stage upload result", "rows_affected", rowsAffected, "batch_date", batch_date)
		}
	}

	// Refresh the Snowpipe with enhanced monitoring
	pipeStartTime := time.Now()
	log.Info("Refreshing Snowpipe for data ingestion", "batch_date", batch_date)

	result, err = db.Exec(fmt.Sprintf(`
		ALTER PIPE %s_pipe REFRESH
		`, TableName))
	if err != nil {
		return 0, fmt.Errorf("Error refreshing Snowpipe for data ingestion (batch: %s): %v", batch_date, err)
	}

	pipeElapsed := time.Since(pipeStartTime)
	log.Info("Snowpipe refresh completed",
		"batch_date", batch_date,
		"refresh_time_seconds", int(pipeElapsed.Seconds()))

	// Log result details if available
	if result != nil {
		if rowsAffected, err := result.RowsAffected(); err == nil {
			log.Debug("Pipe refresh result", "rows_affected", rowsAffected, "batch_date", batch_date)
		}
	}

	totalElapsed := time.Since(startTime)
	log.Info("Snowflake upload completed successfully",
		"batch_date", batch_date,
		"total_time_seconds", int(totalElapsed.Seconds()),
		"documents_processed", stats.DocumentsProcessed,
		"chunks_written", stats.ChunksWritten,
		"success_rate_percent", float64(stats.ChunksWritten)/float64(stats.ChunksWritten+stats.ChunksFailedToWrite)*100)

	return stats.DocumentsProcessed, nil
}

func (s *Snowflake) CreateViews(data execute.RootSchema) error {
	db, err := sql.Open("snowflake", s.dsn)
	if err != nil {
		return fmt.Errorf("Error connecting to database: %v", err)
	}
	if err = bootstrap(db); err != nil {
		return fmt.Errorf("Error bootstrapping database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(fmt.Sprintf(`
	CREATE OR REPLACE SECURE VIEW %s_LATEST_ALL_VERSIONS AS
	SELECT *
	FROM %s ed
	WHERE (ed.TYPE, ed.ID, ed.VERSION, ed.BATCH_DATE) IN (
		SELECT TYPE, ID, VERSION, MAX(BATCH_DATE)
		FROM %s 
		GROUP BY TYPE, ID, VERSION
	)
	`, TableName, TableName, TableName))
	if err != nil {
		return fmt.Errorf("Error creating batch latest view: %v", err)
	}

	_, err = db.Exec(fmt.Sprintf(`
	CREATE OR REPLACE SECURE VIEW %s_LATEST AS
	SELECT *
	FROM %s_LATEST_ALL_VERSIONS ed
	WHERE (ed.TYPE, ed.ID, ed.VERSION) IN (
		SELECT TYPE, ID, MAX(VERSION)
		FROM %s 
		GROUP BY TYPE, ID
	)
	`, TableName, TableName, TableName))
	if err != nil {
		return fmt.Errorf("Error creating latest view: %v", err)
	}

	for key, value := range data {
		log.Infof("Creating Helper Views for `%s`", key)
		create_view(db, key, key, "", value, "data", "")
	}

	return nil
}

func pathToFileURL(path string) string {
	// Replace backslashes with forward slashes
	path = strings.ReplaceAll(path, "\\", "/")

	// Add "file://" prefix
	if !strings.HasPrefix(path, "file://") {
		path = "file://" + path
	}

	// Encode special characters
	u, _ := url.Parse(path)
	return u.String()
}

func create_view(db *sql.DB, docType string, tableName string, parentTable string, record execute.DocumentSchema, root string, flatten string) {

	var columns []string

	columns = append(columns, "id as DOCUMENT_ID")

	if strings.HasPrefix(root, "value:") {
		// special case to pull out the listitem_id for child custom records on list
		columns = append(columns, "value:LISTITEM_ID::string as LISTITEM_ID")
	}

	if parentTable == "" {
		columns = append(columns, "deleted as \"_DELETED\"")
		columns = append(columns, "author as \"_AUTHOR\"")
		columns = append(columns, "version as \"_VERSION\"")
		columns = append(columns, "date as \"_DATE\"")
	}

	for field, metadata := range record {
		if field == "DOCUMENT_ID" {
			continue
		}
		switch metadata.Type {
		case "TEXT", "GUID", "UWI":
			columns = append(columns, fmt.Sprintf("%s:%s::string as %s", root, field, field))
		case "INTEGER":
			columns = append(columns, fmt.Sprintf("%s:%s::int as %s", root, field, field))
		case "DECIMAL":
			columns = append(columns, fmt.Sprintf("%s:%s::float as %s", root, field, field))
		case "BOOLEAN":
			columns = append(columns, fmt.Sprintf("%s:%s::int as %s", root, field, field))
		case "DATETIME":
			columns = append(columns, fmt.Sprintf("%s:%s::date as %s", root, field, field))
		case "DOCUMENT":
			columns = append(columns, fmt.Sprintf("%s:%s:DOCUMENT_ID::string as %s /* References %s.DOCUMENT_ID */", root, field, field, *metadata.DocumentType))
		case "RECORD":
			create_view(db, docType, fmt.Sprintf("%s_%s", tableName, field), tableName, metadata.RecordType, fmt.Sprintf("%s:%s", root, field), flatten)
		case "RECORD LIST":
			// Don't support LIST in LIST
			if !strings.HasPrefix(root, "data") {
				continue
			}
			create_view(db, docType, fmt.Sprintf("%s_%s", tableName, field), tableName, metadata.RecordType, "value", fmt.Sprintf(", LATERAL FLATTEN( INPUT => %s:%s)", root, field))
		default:
			log.Infof("Skipping %s:%s of unknown type %s", tableName, field, metadata.Type)
		}
	}

	cmd := fmt.Sprintf("create or replace secure view %s as select %s from %s_LATEST%s where type='%s'",
		tableName,
		strings.Join(columns, ", "),
		TableName,
		flatten,
		docType)

	if flatten == "" {
		cmd = cmd + " and chunk=0"
	}

	_, err := db.Exec(cmd)
	if err != nil {
		log.Errorf("Error creating %s: %v", tableName, err)
		log.Debug(cmd)
	}
}
