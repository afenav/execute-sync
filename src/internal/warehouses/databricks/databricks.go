package databricks

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/afenav/execute-sync/src/internal/execute"
	"github.com/charmbracelet/log"
	dbsql "github.com/databricks/databricks-sql-go"
)

type Config struct {
	DSN      string
	Host     string
	HttpPath string
	Token    string
	Catalog  string // optional
	Schema   string // optional
}

const TableName = "EXECUTE_DOCUMENTS"

type Databricks struct {
	cfg       Config
	client    *sql.DB
	chunkSize int
}

// fullObjectName returns the fully-qualified name for any table/view given its simple identifier.
func (d *Databricks) fullObjectName(obj string) string {
	if d.cfg.Catalog != "" && d.cfg.Schema != "" {
		return fmt.Sprintf("%s.%s.%s", d.cfg.Catalog, d.cfg.Schema, obj)
	}
	if d.cfg.Schema != "" {
		return fmt.Sprintf("%s.%s", d.cfg.Schema, obj)
	}
	return obj
}

// parseDatabricksDSN parses a Databricks DSN string for both SQL and REST usage.
func parseDatabricksDSN(dsn string) (Config, error) {
	cfg := Config{DSN: dsn}
	if len(dsn) > 12 && strings.HasPrefix(dsn, "databricks://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return cfg, err
		}
		cfg.Host = u.Host
		// Parse token from userinfo
		if u.User != nil {
			if pw, ok := u.User.Password(); ok {
				cfg.Token = pw
			} else {
				cfg.Token = u.User.Username()
			}
		}
		q := u.Query()
		cfg.HttpPath = q.Get("http_path")
		cfg.Catalog = q.Get("catalog")
		cfg.Schema = q.Get("schema")
		return cfg, nil
	}
	// Else, parse key-value format
	for _, part := range strings.Split(dsn, ";") {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(kv[0]))
		val := strings.TrimSpace(kv[1])
		switch key {
		case "host":
			cfg.Host = val
		case "http_path":
			cfg.HttpPath = val
		case "access_token", "token":
			cfg.Token = val
		case "catalog":
			cfg.Catalog = val
		case "schema":
			cfg.Schema = val
		}
	}
	return cfg, nil
}

func NewDatabricks(dsn string, chunkSize int) (*Databricks, error) {
	cfg, err := parseDatabricksDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("invalid Databricks DSN: %w", err)
	}
	port := 443
	host := cfg.Host
	if colon := strings.LastIndex(cfg.Host, ":"); colon != -1 {
		hostOnly := cfg.Host[:colon]
		portStr := cfg.Host[colon+1:]
		if p, err := strconv.Atoi(portStr); err == nil {
			port = p
			host = hostOnly
		}
	}
	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname(host),
		dbsql.WithHTTPPath(cfg.HttpPath),
		dbsql.WithAccessToken(cfg.Token),
		dbsql.WithPort(port),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create Databricks connector: %w", err)
	}
	db := sql.OpenDB(connector)
	return &Databricks{cfg: cfg, client: db, chunkSize: chunkSize}, nil
}

func (d *Databricks) bootstrap() error {
	tableName := d.fullObjectName(TableName)
	log.Debug("Bootstraping table", "table", tableName)
	createTableSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		batch_date TIMESTAMP,
		type STRING,
		id STRING,
		version INT,
		chunk INT,
		author STRING,
		date TIMESTAMP,
		deleted BOOLEAN,
		data STRING
	) USING DELTA`, tableName)
	_, err := d.client.ExecContext(context.Background(), createTableSQL)
	if err != nil {
		return fmt.Errorf("error creating %s table: %w", tableName, err)
	}
	return nil
}

// Upload implements the Database interface. It serializes records to CSV (like Snowflake), uploads to DBFS, and loads into the Databricks table.
func (d *Databricks) Upload(batch_date string, nextRecord func() (map[string]interface{}, error)) (int, error) {
	tableName := d.fullObjectName(TableName)
	// Ensure table exists
	if err := d.bootstrap(); err != nil {
		return 0, err
	}
	tempDir := os.TempDir()
	safeBatchDate := strings.ReplaceAll(strings.ReplaceAll(batch_date, ":", ""), "-", "")
	tmpFile, err := os.CreateTemp(tempDir, fmt.Sprintf("documents_%s*.csv", safeBatchDate))
	if err != nil {
		return 0, fmt.Errorf("error creating temporary file: %v", err)
	}
	defer func() {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
	}()

	log.Debug("Writing to temporary file: ", tmpFile.Name())
	csvWriter := csv.NewWriter(tmpFile)
	csvWriter.Comma = '\t' // use TAB delimiter to avoid comma conflicts
	// No header row; COPY INTO will provide column list
	document_count := 0
	empty_batch := true
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
				if len(list) > d.chunkSize {
					for i := 0; i < len(list); i += d.chunkSize {
						end := i + d.chunkSize
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

			// batch_date column comes from function argument
			batchDateStr := batch_date
			if batchDateStr == "" || batchDateStr == "<nil>" {
				batchDateStr = "NULL"
			}

			// date column comes from $DATE field in the document (string or RFC3339)
			dateStr := "NULL"
			if v, ok := data["$DATE"]; ok {
				switch vv := v.(type) {
				case string:
					if vv != "" {
						// try parse to re-format
						if parsed, err := time.Parse(time.RFC3339, vv); err == nil {
							dateStr = parsed.Format("2006-01-02 15:04:05")
						} else {
							dateStr = vv
						}
					}
				case time.Time:
					dateStr = vv.Format("2006-01-02 15:04:05")
				}
			}

			csvRecord := []string{
				batchDateStr,
				fmt.Sprintf("%v", data["$TYPE"].(string)),
				fmt.Sprintf("%v", data["DOCUMENT_ID"].(string)),
				fmt.Sprintf("%d", int(data["$VERSION"].(float64))),
				fmt.Sprintf("%d", i),
				fmt.Sprintf("%v", data["$AUTHOR_ID"].(string)),
				dateStr,
				fmt.Sprintf("%t", data["$DELETED"].(bool)),
				string(chunkBytes),
			}
			if err := csvWriter.Write(csvRecord); err != nil {
				continue
			}
		}
		document_count += 1
		empty_batch = false
	}
	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return 0, fmt.Errorf("error finalizing CSV file: %v", err)
	}
	if !empty_batch {
		dbfsPath := fmt.Sprintf("/tmp/%s_%s-%d.csv", TableName, safeBatchDate, time.Now().UnixNano())
		if err := d.uploadToDBFS(tmpFile.Name(), dbfsPath); err != nil {
			return 0, fmt.Errorf("upload to DBFS failed: %w", err)
		}
		log.Debug("Uploading batch to Databricks: ", tableName)
		query := fmt.Sprintf(`COPY INTO %s (batch_date, type, id, version, chunk, author, date, deleted, data)
		FROM 'dbfs:%s'
		FILEFORMAT = CSV
		FORMAT_OPTIONS('header' = 'false', 'delimiter' = '\t', 'timestampFormat' = 'yyyy-MM-dd HH:mm:ss', 'quote' = '"', 'escape' = '"', 'nullValue' = 'NULL')`, tableName, dbfsPath)
		if _, err := d.client.ExecContext(context.Background(), query); err != nil {
			return 0, fmt.Errorf("COPY INTO failed: %w", err)
		}
	}
	return document_count, nil
}

func (d *Databricks) Prune() error {
	if err := d.bootstrap(); err != nil {
		return err
	}
	tableName := d.fullObjectName(TableName)
	pruneSQL := fmt.Sprintf(`DELETE FROM %s t
WHERE EXISTS (
  SELECT 1 FROM (
    SELECT type, id, version, MAX(batch_date) AS max_batch
    FROM %s
    GROUP BY type, id, version
  ) latest
  WHERE t.type = latest.type
    AND t.id = latest.id
    AND t.version = latest.version
    AND t.batch_date < latest.max_batch
)`, tableName, tableName)

	_, err := d.client.ExecContext(context.Background(), pruneSQL)
	return err
}

func (d *Databricks) CreateViews(data execute.RootSchema) error {
	if err := d.bootstrap(); err != nil {
		return fmt.Errorf("error bootstrapping database: %v", err)
	}

	// Build fully qualified base table and view names
	baseTable := d.fullObjectName(TableName)
	viewAllVersions := d.fullObjectName(TableName + "_LATEST_ALL_VERSIONS")
	viewLatest := d.fullObjectName(TableName + "_LATEST")

	ctx := context.Background()

	// _LATEST_ALL_VERSIONS view – latest batch for every (type,id,version)
	log.Debug("Creating view", "view", viewAllVersions)
	queryAll := fmt.Sprintf(`CREATE OR REPLACE VIEW %s AS
SELECT ed.*
FROM %s ed
INNER JOIN (
  SELECT type, id, version, MAX(batch_date) AS batch_date
  FROM %s
  GROUP BY type, id, version
) latest
ON ed.type = latest.type
 AND ed.id = latest.id
 AND ed.version = latest.version
 AND ed.batch_date = latest.batch_date`, viewAllVersions, baseTable, baseTable)
	if _, err := d.client.ExecContext(ctx, queryAll); err != nil {
		return fmt.Errorf("error creating %s view: %w", viewAllVersions, err)
	}

	// _LATEST view – latest version per (type,id)
	log.Debug("Creating view", "view", viewLatest)
	queryLatest := fmt.Sprintf(`CREATE OR REPLACE VIEW %s AS
SELECT ed.*
FROM %s ed
INNER JOIN (
  SELECT type, id, MAX(version) AS version
  FROM %s
  GROUP BY type, id
) latest
ON ed.type = latest.type
 AND ed.id = latest.id
 AND ed.version = latest.version`, viewLatest, viewAllVersions, baseTable)
	if _, err := d.client.ExecContext(ctx, queryLatest); err != nil {
		return fmt.Errorf("error creating %s view: %w", viewLatest, err)
	}
	for key, value := range data {
		log.Infof("Creating Helper Views for `%s`", key)
		d.create_view(key, key, "", value, "data", "$", "")
	}

	return nil
}

func (d *Databricks) create_view(docType string, viewName string, parentTable string, record execute.DocumentSchema, root string, path string, flatten string) {

	var columns []string

	columns = append(columns, "id as DOCUMENT_ID")

	if root == "value" && path != "$" {
		// special case to pull out the listitem_id for child custom records on list
		columns = append(columns, "CAST(get_json_object(value, '$.LISTITEM_ID') AS string) AS LISTITEM_ID")
	}

	if parentTable == "" {
		columns = append(columns, "deleted as _DELETED")
		columns = append(columns, "author as _AUTHOR")
		columns = append(columns, "version as _VERSION")
		columns = append(columns, "date as _DATE")
	}

	for field, metadata := range record {
		if field == "DOCUMENT_ID" {
			continue
		}
		switch metadata.Type {
		case "TEXT", "GUID", "UWI":
			columns = append(columns, fmt.Sprintf("CAST(get_json_object(%s, '%s.%s') AS string) AS %s", root, path, field, field))
		case "INTEGER":
			columns = append(columns, fmt.Sprintf("CAST(get_json_object(%s, '%s.%s') AS int) AS %s", root, path, field, field))
		case "DECIMAL":
			columns = append(columns, fmt.Sprintf("CAST(get_json_object(%s, '%s.%s') AS float) AS %s", root, path, field, field))
		case "BOOLEAN":
			columns = append(columns, fmt.Sprintf("CAST(get_json_object(%s, '%s.%s') AS int) AS %s", root, path, field, field))
		case "DATETIME":
			columns = append(columns, fmt.Sprintf("CAST(get_json_object(%s, '%s.%s') AS date) AS %s", root, path, field, field))
		case "DOCUMENT":
			columns = append(columns, fmt.Sprintf("CAST(get_json_object(%s, '%s.%s.DOCUMENT_ID') AS string) AS %s /* References %s.DOCUMENT_ID */", root, path, field, field, *metadata.DocumentType))
		case "RECORD":
			d.create_view(docType, fmt.Sprintf("%s_%s", viewName, field), viewName, metadata.RecordType, root, fmt.Sprintf("%s.%s", path, field), flatten)
		case "RECORD LIST":
			// Don't support LIST in LIST
			if root != "data" {
				continue
			}
			jsonPath := fmt.Sprintf("%s.%s", path, field)
			explodeClause := fmt.Sprintf(" lateral view explode_outer(from_json(get_json_object(%s, '%s'), 'array<string>')) AS value", root, jsonPath)
			d.create_view(docType, fmt.Sprintf("%s_%s", viewName, field), viewName, metadata.RecordType, "value", "$", explodeClause)
		default:
			log.Infof("Skipping %s:%s of unknown type %s", viewName, field, metadata.Type)
		}
	}

	cmd := fmt.Sprintf("create or replace view %s as select %s from %s_LATEST%s where type='%s'",
		d.fullObjectName(viewName),
		strings.Join(columns, ", "),
		d.fullObjectName(TableName),
		flatten,
		docType)

	if flatten == "" {
		cmd = cmd + " and chunk=0"
	}

	log.Debug("Creating view", "view", viewName)
	_, err := d.client.ExecContext(context.Background(), cmd)
	if err != nil {
		log.Errorf("Error creating %s: %v", viewName, err)
		log.Debug(cmd)
	}
}

// uploadToDBFS uploads a local file to DBFS via Databricks REST API.
func (d *Databricks) uploadToDBFS(localPath, dbfsPath string) error {
	log.Debug("Uploading to DBFS: ", dbfsPath)
	file, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer file.Close()

	url := fmt.Sprintf("https://%s/api/2.0/dbfs/put", d.cfg.Host)
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	_ = writer.WriteField("path", dbfsPath)
	_ = writer.WriteField("overwrite", "true")
	part, _ := writer.CreateFormFile("file", filepath.Base(localPath))
	if _, err := io.Copy(part, file); err != nil {
		return err
	}
	writer.Close()

	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+d.cfg.Token)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("dbfs put failed: %s", string(b))
	}
	return nil
}
