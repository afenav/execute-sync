package sqlserver

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/afenav/execute-sync/src/internal/execute"
	"github.com/charmbracelet/log"
	_ "github.com/denisenkom/go-mssqldb"
)

const TableName string = "EXECUTE_DOCUMENTS"

type SQLServer struct {
	dsn       string
	chunkSize int
}

func NewSQLServer(dsn string, chunkSize int) (*SQLServer, error) {
	return &SQLServer{
		dsn:       dsn,
		chunkSize: chunkSize,
	}, nil
}

// bootstrap initializes the SQL Server database with the required objects
func bootstrap(db *sql.DB) error {
	// Create the main table if it doesn't exist
	_, err := db.Exec(fmt.Sprintf(`
	IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[%s]') AND type in (N'U'))
	BEGIN
		CREATE TABLE [%s] (
			BATCH_DATE DATETIME2 NOT NULL,
			TYPE NVARCHAR(50) NOT NULL,
			ID NVARCHAR(50) NOT NULL,
			VERSION INT NOT NULL,
			CHUNK INT NOT NULL,
			AUTHOR NVARCHAR(50),
			DATE DATETIME2 NOT NULL,
			DELETED BIT NOT NULL,
			DATA NVARCHAR(MAX) NOT NULL,
			CONSTRAINT [PK_%s] PRIMARY KEY CLUSTERED (BATCH_DATE, TYPE, ID, VERSION, CHUNK)
		)
	END
	`, TableName, TableName, TableName))

	if err != nil {
		return fmt.Errorf("error creating table: %v", err)
	}

	return nil
}

// Prune removes old data that is no longer needed
func (s *SQLServer) Prune() error {
	db, err := sql.Open("sqlserver", s.dsn)
	if err != nil {
		return fmt.Errorf("error connecting to database: %v", err)
	}
	if err = bootstrap(db); err != nil {
		return fmt.Errorf("error bootstrapping database: %v", err)
	}
	defer db.Close()

	// Delete records that are not the latest version for each TYPE, ID, VERSION
	_, err = db.Exec(fmt.Sprintf(`
	DELETE FROM [%s]
	WHERE NOT EXISTS (
		SELECT 1 FROM [%s] t2
		WHERE [%s].TYPE = t2.TYPE
		  AND [%s].ID = t2.ID
		  AND [%s].VERSION = t2.VERSION
		  AND [%s].BATCH_DATE = (
			SELECT MAX(BATCH_DATE) FROM [%s] t3
			WHERE t3.TYPE = t2.TYPE
			  AND t3.ID = t2.ID
			  AND t3.VERSION = t2.VERSION
		)
	)
	`, TableName, TableName, TableName, TableName, TableName, TableName, TableName))

	if err != nil {
		return fmt.Errorf("error pruning data: %v", err)
	}

	return nil
}

// Upload uploads records to SQL Server
func (s *SQLServer) Upload(batch_date string, nextRecord func() (map[string]interface{}, error)) (int, error) {
	db, err := sql.Open("sqlserver", s.dsn)
	if err != nil {
		return 0, fmt.Errorf("error connecting to database: %v", err)
	}
	if err = bootstrap(db); err != nil {
		return 0, fmt.Errorf("error bootstrapping database: %v", err)
	}
	defer db.Close()

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		return 0, fmt.Errorf("error beginning transaction: %v", err)
	}

	// Prepare insert statement
	stmt, err := tx.Prepare(fmt.Sprintf(`
	INSERT INTO [%s] (
		BATCH_DATE, TYPE, ID, VERSION, CHUNK, AUTHOR, DATE, DELETED, DATA
	) VALUES (
		@p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9
	)`, TableName))

	if err != nil {
		tx.Rollback()
		return 0, fmt.Errorf("error preparing statement: %v", err)
	}
	defer stmt.Close()

	count := 0

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
			_, err = stmt.Exec(
				batch_date,
				data["$TYPE"].(string),
				data["DOCUMENT_ID"].(string),
				int(data["$VERSION"].(float64)),
				i,
				data["$AUTHOR_ID"].(string),
				data["$DATE"].(string),
				data["$DELETED"].(bool),
				string(chunkBytes))

			if err != nil {
				log.Infof("Error writing record to SQL: %s\n", err)
				tx.Rollback()
				return count, err
			}
		}

		count += 1

	}

	// Commit transaction
	if err = tx.Commit(); err != nil {
		tx.Rollback()
		return count, fmt.Errorf("error committing transaction: %v", err)
	}

	return count, nil
}

func (s *SQLServer) CreateViews(data execute.RootSchema) error {
	db, err := sql.Open("sqlserver", s.dsn)
	if err != nil {
		return fmt.Errorf("error connecting to database: %v", err)
	}
	if err = bootstrap(db); err != nil {
		return fmt.Errorf("error bootstrapping database: %v", err)
	}
	defer db.Close()

	// Drop and create _LATEST_ALL_VERSIONS view
	_, err = db.Exec(fmt.Sprintf(`
	CREATE OR ALTER VIEW %s_LATEST_ALL_VERSIONS AS
	SELECT ed.*
	FROM %s ed
	INNER JOIN (
		SELECT TYPE, ID, VERSION, MAX(BATCH_DATE) AS BATCH_DATE
		FROM %s
		GROUP BY TYPE, ID, VERSION
	) latest
	ON ed.TYPE = latest.TYPE
	   AND ed.ID = latest.ID
	   AND ed.VERSION = latest.VERSION
	   AND ed.BATCH_DATE = latest.BATCH_DATE;
	`, TableName, TableName, TableName))
	if err != nil {
		return fmt.Errorf("error creating batch latest view: %v", err)
	}

	// Drop and create _LATEST view
	_, err = db.Exec(fmt.Sprintf(`
	CREATE OR ALTER VIEW %s_LATEST AS
	SELECT ed.*
	FROM %s_LATEST_ALL_VERSIONS ed
	INNER JOIN (
		SELECT TYPE, ID, MAX(VERSION) AS VERSION
		FROM %s
		GROUP BY TYPE, ID
	) latest
	ON ed.TYPE = latest.TYPE
	   AND ed.ID = latest.ID
	   AND ed.VERSION = latest.VERSION;
	`, TableName, TableName, TableName))
	if err != nil {
		return fmt.Errorf("error creating latest view: %v", err)
	}

	for key, value := range data {
		log.Infof("Creating Helper View `%s`", key)
		create_view(db, key, key, "", value, "data", "$", "")
	}

	return nil
}

func create_view(db *sql.DB, docType string, tableName string, parentTable string, record execute.DocumentSchema, dataField string, root string, flatten string) {

	var columns []string

	columns = append(columns, "id as DOCUMENT_ID")

	if dataField == "value" {
		// special case to pull out the listitem_id for child custom records on list
		columns = append(columns, "CAST(JSON_VALUE(value, '$.LISTITEM_ID') as nvarchar) as LISTITEM_ID")
	}

	if parentTable == "" {
		columns = append(columns, "deleted as [_DELETED]")
		columns = append(columns, "author as [_AUTHOR]")
		columns = append(columns, "version as [_VERSION]")
		columns = append(columns, "date as [_DATE]")
	}

	for field, metadata := range record {
		if field == "DOCUMENT_ID" {
			continue
		}
		if field == "LISTITEM_ID" {
			continue
		}
		jsonPath := root + "." + field
		switch metadata.Type {
		case "TEXT", "GUID", "UWI":
			columns = append(columns, fmt.Sprintf("CAST(JSON_VALUE(%s, '%s') AS NVARCHAR(255)) as %s", dataField, jsonPath, field))
		case "INTEGER":
			columns = append(columns, fmt.Sprintf("CAST(JSON_VALUE(%s, '%s') AS INT) as %s", dataField, jsonPath, field))
		case "DECIMAL":
			columns = append(columns, fmt.Sprintf("CAST(JSON_VALUE(%s, '%s') AS FLOAT) as %s", dataField, jsonPath, field))
		case "BOOLEAN":
			columns = append(columns, fmt.Sprintf("CAST(JSON_VALUE(%s, '%s') AS BIT) as %s", dataField, jsonPath, field))
		case "DATETIME":
			columns = append(columns, fmt.Sprintf("CAST(JSON_VALUE(%s, '%s') AS DATETIME2) as %s", dataField, jsonPath, field))
		case "DOCUMENT":
			columns = append(columns, fmt.Sprintf("CAST(JSON_VALUE(%s, '%s.DOCUMENT_ID') AS NVARCHAR(255)) as %s /* References %s.DOCUMENT_ID */", dataField, jsonPath, field, *metadata.DocumentType))
		case "RECORD":
			create_view(db, docType, fmt.Sprintf("%s_%s", tableName, field), tableName, metadata.RecordType, dataField, fmt.Sprintf("%s.%s", root, field), flatten)
		case "RECORD LIST":
			// Don't support LIST in LIST
			if dataField == "value" {
				continue
			}
			create_view(db, docType, fmt.Sprintf("%s_%s", tableName, field), tableName, metadata.RecordType, "value", "$", fmt.Sprintf(" OUTER APPLY OPENJSON(data, '%s') AS value", jsonPath))
		default:
			log.Infof("Skipping %s:%s of unknown type %s", tableName, field, metadata.Type)
		}
	}

	cmd := fmt.Sprintf("create or alter view [%s] as select %s from %s_LATEST%s where %s_LATEST.type='%s'",
		tableName,
		strings.Join(columns, ", "),
		TableName,
		flatten,
		TableName,
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
