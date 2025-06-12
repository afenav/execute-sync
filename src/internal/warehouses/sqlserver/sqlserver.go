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
	IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[%s]') AND type in (N'U'))
	BEGIN
		CREATE TABLE [dbo].[%s] (
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
	DELETE FROM [dbo].[%s]
	WHERE (TYPE, ID, VERSION, BATCH_DATE) NOT IN (
		SELECT TYPE, ID, VERSION, MAX(BATCH_DATE)
		FROM [dbo].[%s]
		GROUP BY TYPE, ID, VERSION
	)
	`, TableName, TableName))

	if err != nil {
		return fmt.Errorf("error pruning data: %v", err)
	}

	_, err = db.Exec(fmt.Sprintf(`
	DELETE FROM [dbo].[%s] 
	WHERE DELETED = 1 AND
		(TYPE, ID, BATCH_DATE) IN (
		SELECT a.TYPE, a.ID, MAX(a.BATCH_DATE)
		FROM [dbo].[%s] a
		WHERE a.DELETED = 1
		GROUP BY a.TYPE, a.ID
	)
	`, TableName, TableName))

	if err != nil {
		return fmt.Errorf("error pruning deleted records: %v", err)
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
	INSERT INTO [dbo].[%s] (
		BATCH_DATE, TYPE, ID, VERSION, CHUNK, AUTHOR, DATE, DELETED, DATA
	) VALUES (
		?, ?, ?, ?, ?, ?, ?, ?, ?
	)`, TableName))
	
	if err != nil {
		tx.Rollback()
		return 0, fmt.Errorf("error preparing statement: %v", err)
	}
	defer stmt.Close()
	
	count := 0
	for {
		record, err := nextRecord()
		if err != nil {
			tx.Rollback()
			return count, err
		}
		
		// End of records
		if record == nil {
			break
		}
		
		// Extract fields from the record
		docType, ok := record["type"].(string)
		if !ok {
			tx.Rollback()
			return count, fmt.Errorf("error extracting type from record")
		}
		
		id, ok := record["id"].(string)
		if !ok {
			tx.Rollback()
			return count, fmt.Errorf("error extracting id from record")
		}
		
		version, ok := record["version"].(float64)
		if !ok {
			tx.Rollback()
			return count, fmt.Errorf("error extracting version from record")
		}
		
		author, _ := record["author"].(string)
		
		date, ok := record["date"].(string)
		if !ok {
			tx.Rollback()
			return count, fmt.Errorf("error extracting date from record")
		}
		
		deleted, _ := record["deleted"].(bool)
		
		// Process data into chunks
		data := record["data"]
		jsonData, err := json.Marshal(data)
		if err != nil {
			tx.Rollback()
			return count, fmt.Errorf("error marshalling data: %v", err)
		}
		
		// Handle data in chunks if it's too large
		dataStr := string(jsonData)
		chunkCount := 1
		
		if len(dataStr) > s.chunkSize {
			log.Debug("Large document detected, splitting into chunks", "type", docType, "id", id, "size", len(dataStr))
			chunkCount = (len(dataStr) + s.chunkSize - 1) / s.chunkSize
		}
		
		for chunk := 0; chunk < chunkCount; chunk++ {
			start := chunk * s.chunkSize
			end := (chunk + 1) * s.chunkSize
			if end > len(dataStr) {
				end = len(dataStr)
			}
			
			chunkData := dataStr[start:end]
			
			_, err = stmt.Exec(
				batch_date,
				docType,
				id,
				int(version),
				chunk,
				author,
				date,
				deleted,
				chunkData,
			)
			
			if err != nil {
				tx.Rollback()
				return count, fmt.Errorf("error inserting record: %v", err)
			}
			
			count++
		}
	}
	
	// Commit transaction
	if err = tx.Commit(); err != nil {
		tx.Rollback()
		return count, fmt.Errorf("error committing transaction: %v", err)
	}
	
	return count, nil
}

// CreateViews creates views for the data
func (s *SQLServer) CreateViews(root execute.RootSchema) error {
	db, err := sql.Open("sqlserver", s.dsn)
	if err != nil {
		return fmt.Errorf("error connecting to database: %v", err)
	}
	if err = bootstrap(db); err != nil {
		return fmt.Errorf("error bootstrapping database: %v", err)
	}
	defer db.Close()

	// First create a base view of the latest data
	_, err = db.Exec(fmt.Sprintf(`
	IF EXISTS (SELECT * FROM sys.views WHERE name = 'EXECUTE_LATEST')
	    DROP VIEW EXECUTE_LATEST;
	`))
	if err != nil {
		return fmt.Errorf("error dropping base view: %v", err)
	}

	_, err = db.Exec(fmt.Sprintf(`
	CREATE VIEW EXECUTE_LATEST AS
	SELECT 
		d1.TYPE,
		d1.ID,
		d1.VERSION,
		d1.AUTHOR,
		d1.DATE,
		d1.BATCH_DATE,
		d1.DELETED,
		(
			SELECT STRING_AGG(d2.DATA, '') 
			FROM [dbo].[%s] d2
			WHERE 
				d2.TYPE = d1.TYPE AND 
				d2.ID = d1.ID AND
				d2.VERSION = d1.VERSION AND
				d2.BATCH_DATE = d1.BATCH_DATE
			ORDER BY d2.CHUNK
		) AS DATA
	FROM [dbo].[%s] d1
	WHERE (d1.TYPE, d1.ID, d1.VERSION, d1.BATCH_DATE) IN (
		SELECT TYPE, ID, VERSION, MAX(BATCH_DATE)
		FROM [dbo].[%s]
		GROUP BY TYPE, ID, VERSION
	)
	AND d1.CHUNK = 0
	AND d1.DELETED = 0
	`, TableName, TableName, TableName))

	if err != nil {
		return fmt.Errorf("error creating base view: %v", err)
	}

	// Create views for each document type
	for docType, schema := range root {
		create_view(db, docType, docType, "EXECUTE_LATEST", schema, "", "")
	}

	return nil
}

// create_view creates a view for a specific document type
func create_view(db *sql.DB, docType string, tableName string, parentTable string, record execute.DocumentSchema, root string, flatten string) {
	safeTable := sanitizeName(tableName)
	
	// Drop existing view
	_, err := db.Exec(fmt.Sprintf("IF EXISTS (SELECT * FROM sys.views WHERE name = '%s') DROP VIEW %s", safeTable, safeTable))
	if err != nil {
		log.Error("Error dropping view", "type", docType, "error", err)
		return
	}
	
	// Build the columns list
	columnsList := []string{
		"ID",
		"VERSION",
		"AUTHOR",
		"DATE",
		"BATCH_DATE",
	}
	
	// Add fields
	for field, fieldMeta := range record {
		if fieldMeta.Type != "RECORD" {
			if root == "" {
				columnsList = append(columnsList, fmt.Sprintf("JSON_VALUE(DATA, '$.%s') as %s", field, sanitizeName(field)))
			} else {
				columnsList = append(columnsList, fmt.Sprintf("JSON_VALUE(DATA, '$.%s.%s') as %s", root, field, sanitizeName(field)))
			}
		}
	}
	
	// Create the view
	_, err = db.Exec(fmt.Sprintf(`
	CREATE VIEW %s AS
	SELECT 
		%s
	FROM %s
	WHERE TYPE = '%s'
	`, safeTable, strings.Join(columnsList, ",\n\t\t"), parentTable, docType))
	
	if err != nil {
		log.Error("Error creating view", "type", docType, "error", err)
		return
	}
	
	log.Info("Created view", "view", safeTable)
	
	// Process nested objects
	for field, fieldMeta := range record {
		if fieldMeta.Type == "RECORD" && fieldMeta.RecordType != nil {
			fieldRoot := field
			if root != "" {
				fieldRoot = root + "." + field
			}
			
			nestedSchema := execute.DocumentSchema{}
			for subField, subMeta := range fieldMeta.RecordType {
				nestedSchema[subField] = subMeta
			}
			
			nestedTableName := tableName + "_" + field
			create_view(db, docType, nestedTableName, parentTable, nestedSchema, fieldRoot, "")
		}
	}
}

// sanitizeName sanitizes a name to be used as an SQL Server identifier
func sanitizeName(name string) string {
	return strings.Replace(name, "-", "_", -1)
}
