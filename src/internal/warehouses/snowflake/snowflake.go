package snowflake

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/afenav/execute-sync/src/internal/execute"
	"github.com/gofiber/fiber/v2/log"
	_ "github.com/snowflakedb/gosnowflake"
)

const TableName string = "EXECUTE_DOCUMENTS"

type Snowflake struct {
	dsn       string
	chunkSize int
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
	db, err := sql.Open("snowflake", s.dsn)
	if err != nil {
		return 0, fmt.Errorf("Error connecting to database: %v", err)
	}
	if err = bootstrap(db); err != nil {
		return 0, fmt.Errorf("Error bootstrapping database: %v", err)
	}
	defer db.Close()

	document_count := 0

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
			}
		}

		// Skip empty records
		if data == nil {
			continue
		}

		// Apply chunking
		var chunks []map[string]interface{}

		// Iterate through the top-level keys
		for key, value := range data {
			// Is this a list key?
			if list, ok := value.([]interface{}); ok {
				// Does this list have #items > chunk size?
				if len(list) > s.chunkSize {
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

		for i := 0; i < len(chunks); i++ {
			chunkBytes, _ := json.Marshal(chunks[i])
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

			// Write the record to the CSV
			if err := csvWriter.Write(csvRecord); err != nil {
				log.Infof("Error writing record to CSV: %s\n", err)
				continue
			}
		}

		// Keep track of the number of documents processed in this run
		document_count += 1
		empty_batch = false

	}

	// Flush any remaining data to the CSV file
	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return 0, fmt.Errorf("Error finalizing CSV file: %v", err)
	}

	// Don't push an empty batch to Snowflake.  That's silly
	if !empty_batch {
		// Upload the temporary CSV file to the Snowflake stage
		log.Debug("Uploading CSV to Snowflake Stage")

		putCommand := fmt.Sprintf("PUT '%s' @%s_stage", pathToFileURL(tempFile.Name()), TableName)
		_, err = db.Exec(putCommand)
		if err != nil {
			return 0, fmt.Errorf("Error uploading file to Snowflake stage: %v", err)
		}

		// Merge from Stage into the TableName
		log.Debug("Refreshing the Snowpipe")
		_, err = db.Exec(fmt.Sprintf(`
		ALTER PIPE %s_pipe REFRESH
		`, TableName))
		if err != nil {
			return 0, fmt.Errorf("Error ingesting data: %v", err)
		}
	}

	return document_count, nil
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
		log.Infof("Creating Helper View `%s`", key)
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
		log.Errorf("Error creating %s: %w", tableName, err)
		log.Debug(cmd)
	}
}
