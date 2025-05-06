package sqlite

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/afenav/execute-sync/src/internal/execute"
	"github.com/gofiber/fiber/v2/log"
	_ "github.com/mattn/go-sqlite3"
	_ "modernc.org/sqlite"
)

const SQLiteTableName string = "EXECUTE_DOCUMENTS"

type SQLite struct {
	dsn       string
	provider  string
	chunkSize int
}

func NewSQLite(provider string, dsn string, chunkSize int) (*SQLite, error) {
	return &SQLite{
		dsn:       dsn,
		chunkSize: chunkSize,
		provider:  provider,
	}, nil
}

func sqliteBootstrap(db *sql.DB) error {
	_, err := db.Exec(fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		BATCH_DATE TEXT NOT NULL,
		TYPE TEXT NOT NULL,
		ID TEXT NOT NULL,
		VERSION INTEGER NOT NULL,
		CHUNK INTEGER NOT NULL,
		AUTHOR TEXT,
		DATE TEXT NOT NULL,
		DELETED BOOLEAN NOT NULL,
		DATA TEXT NOT NULL,
		PRIMARY KEY (BATCH_DATE, TYPE, ID, VERSION, CHUNK)
	);
	`, SQLiteTableName))
	if err != nil {
		return fmt.Errorf("Error creating table: %v", err)
	}
	return nil
}

func (s *SQLite) Prune() error {
	db, err := sql.Open(s.provider, s.dsn)
	if err != nil {
		return fmt.Errorf("Error connecting to database: %v", err)
	}
	defer db.Close()
	if err = sqliteBootstrap(db); err != nil {
		return fmt.Errorf("Error bootstrapping database: %v", err)
	}

	_, err = db.Exec(fmt.Sprintf(`
	DELETE FROM %s
	WHERE (TYPE, ID, VERSION, BATCH_DATE) NOT IN (
		SELECT TYPE, ID, VERSION, MAX(BATCH_DATE)
		FROM %s
		GROUP BY TYPE, ID, VERSION
	)
	`, SQLiteTableName, SQLiteTableName))
	if err != nil {
		return err
	}
	return nil
}

func (s *SQLite) Upload(batch_date string, nextRecord func() (map[string]interface{}, error)) (int, error) {
	db, err := sql.Open(s.provider, s.dsn)
	if err != nil {
		return 0, fmt.Errorf("Error connecting to database: %v", err)
	}
	defer db.Close()
	if err = sqliteBootstrap(db); err != nil {
		return 0, fmt.Errorf("Error bootstrapping database: %v", err)
	}

	document_count := 0
	tx, err := db.Begin()
	if err != nil {
		return 0, err
	}
	stmt, err := tx.Prepare(fmt.Sprintf(`
	INSERT OR REPLACE INTO %s (BATCH_DATE, TYPE, ID, VERSION, CHUNK, AUTHOR, DATE, DELETED, DATA)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, SQLiteTableName))
	if err != nil {
		tx.Rollback()
		return 0, err
	}
	defer stmt.Close()

	for {
		data, err := nextRecord()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
		}
		if data == nil {
			continue
		}
		var chunks []map[string]interface{}
		for key, value := range data {
			if list, ok := value.([]interface{}); ok {
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
					delete(data, key)
				}
			}
		}
		chunks = append([]map[string]interface{}{data}, chunks...)
		for i := 0; i < len(chunks); i++ {
			chunkBytes, _ := json.Marshal(chunks[i])
			_, err := stmt.Exec(
				batch_date,
				data["$TYPE"].(string),
				data["DOCUMENT_ID"].(string),
				int(data["$VERSION"].(float64)),
				i,
				data["$AUTHOR_ID"].(string),
				data["$DATE"].(string),
				data["$DELETED"].(bool),
				string(chunkBytes),
			)
			if err != nil {
				log.Infof("Error inserting record: %s\n", err)
				continue
			}
		}
		document_count += 1
	}
	err = tx.Commit()
	if err != nil {
		return 0, err
	}
	return document_count, nil
}

func (s *SQLite) CreateViews(data execute.RootSchema) error {
	db, err := sql.Open(s.provider, s.dsn)
	if err != nil {
		return fmt.Errorf("Error connecting to database: %v", err)
	}
	defer db.Close()
	if err = sqliteBootstrap(db); err != nil {
		return fmt.Errorf("Error bootstrapping database: %v", err)
	}

	_, err = db.Exec(fmt.Sprintf(`
	CREATE VIEW IF NOT EXISTS %s_LATEST_ALL_VERSIONS AS
	SELECT * FROM %s ed
	WHERE (ed.TYPE, ed.ID, ed.VERSION, ed.BATCH_DATE) IN (
		SELECT TYPE, ID, VERSION, MAX(BATCH_DATE)
		FROM %s
		GROUP BY TYPE, ID, VERSION
	)
	`, SQLiteTableName, SQLiteTableName, SQLiteTableName))
	if err != nil {
		return fmt.Errorf("Error creating batch latest view: %v", err)
	}

	_, err = db.Exec(fmt.Sprintf(`
	CREATE VIEW IF NOT EXISTS %s_LATEST AS
	SELECT * FROM %s_LATEST_ALL_VERSIONS ed
	WHERE (ed.TYPE, ed.ID, ed.VERSION) IN (
		SELECT TYPE, ID, MAX(VERSION)
		FROM %s
		GROUP BY TYPE, ID
	)
	`, SQLiteTableName, SQLiteTableName, SQLiteTableName))
	if err != nil {
		return fmt.Errorf("Error creating latest view: %v", err)
	}

	for key, value := range data {
		log.Infof("Creating Helper View `%s`", key)
		create_view(db, key, key, "", value, "DATA", "$", "")
	}
	return nil
}

func create_view(db *sql.DB, docType string, tableName string, parentTable string, record execute.DocumentSchema, jsonField string, root string, flatten string) {
	var columns []string

	columns = append(columns, fmt.Sprintf("%s_LATEST.id as DOCUMENT_ID", SQLiteTableName))

	if flatten != "" && root != "$" {
		// special case to pull out the listitem_id for child custom records on list
		columns = append(columns, fmt.Sprintf("json_extract(%s, '$.LISTITEM_ID') as LISTITEM_ID", jsonField))
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
			columns = append(columns, fmt.Sprintf("json_extract(%s, '%s.%s') as %s", jsonField, root, field, field))
		case "INTEGER":
			columns = append(columns, fmt.Sprintf("json_extract(%s, '%s.%s') as %s", jsonField, root, field, field))
		case "DECIMAL":
			columns = append(columns, fmt.Sprintf("json_extract(%s, '%s.%s') as %s", jsonField, root, field, field))
		case "BOOLEAN":
			columns = append(columns, fmt.Sprintf("json_extract(%s, '%s.%s') as %s", jsonField, root, field, field))
		case "DATETIME":
			columns = append(columns, fmt.Sprintf("json_extract(%s, '%s.%s') as %s", jsonField, root, field, field))
		case "DOCUMENT":
			columns = append(columns, fmt.Sprintf("json_extract(%s, '%s.%s.DOCUMENT_ID') as %s", jsonField, root, field, field))
		case "RECORD":
			create_view(db, docType, fmt.Sprintf("%s_%s", tableName, field), tableName, metadata.RecordType, jsonField, fmt.Sprintf("%s.%s", root, field), flatten)
		case "RECORD LIST":
			// Don't support LIST in LIST
			if jsonField != "DATA" {
				continue
			}
			create_view(db, docType, fmt.Sprintf("%s_%s", tableName, field), tableName, metadata.RecordType, "value", "$", fmt.Sprintf(", json_each(DATA,'%s.%s')", root, field))
		default:
			log.Infof("Skipping %s:%s of unknown type %s", tableName, field, metadata.Type)
		}
	}
	cmd := fmt.Sprintf("DROP VIEW IF EXISTS %s", tableName)
	_, err := db.Exec(cmd)
	if err != nil {
		log.Errorf("Error dropping %s: %v", tableName, err)
		log.Debug(cmd)
	}

	cmd = fmt.Sprintf("CREATE VIEW %s as SELECT %s FROM %s_LATEST%s WHERE %s_LATEST.TYPE='%s'",
		tableName,
		strings.Join(columns, ", "),
		SQLiteTableName,
		flatten,
		SQLiteTableName,
		docType)

	if flatten == "" {
		cmd = cmd + " and chunk=0"
	}

	_, err = db.Exec(cmd)
	if err != nil {
		log.Errorf("Error creating %s: %v", tableName, err)
		log.Debug(cmd)
	}
}
