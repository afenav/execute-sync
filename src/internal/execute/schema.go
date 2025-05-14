package execute

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/afenav/execute-sync/src/internal/config"
	"github.com/gofiber/fiber/v2/log"
)

// FieldMetadata represents metadata for a single field.
type FieldMetadata struct {
	Name         string                   `json:"NAME"`
	Active       bool                     `json:"ACTIVE"`
	Type         string                   `json:"TYPE"`
	Nullable     bool                     `json:"NULLABLE"`
	Size         *int                     `json:"SIZE,omitempty"`          // Optional
	RecordType   map[string]FieldMetadata `json:"RECORD_TYPE,omitempty"`   // For nested records
	Formula      *string                  `json:"FORMULA,omitempty"`       // Optional
	DocumentType *string                  `json:"DOCUMENT_TYPE,omitempty"` // For document references
	DateUnzoned  *bool                    `json:"DATE_UNZONED,omitempty"`  // Optional for datetime
}

// DocumentSchema represents the schema of a document.
type DocumentSchema map[string]FieldMetadata

// RootSchema represents the entire JSON structure.
type RootSchema map[string]DocumentSchema

// FetchSchema retrieves the schema of documents from the Execute API.
// It takes a configuration object `cfg` containing the API endpoint and credentials.
// The function returns a `RootSchema` representing the document schema and an error if any occurs.
func FetchSchema(cfg config.Config) (RootSchema, error) {
	client := &http.Client{Timeout: 30 * time.Second}

	// Parse the base URL
	parsedURL, err := url.Parse(cfg.ExecuteURL)
	if err != nil {
		return nil, fmt.Errorf("parsing execute URL: %v", err)
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
		return nil, fmt.Errorf("creating request: %v", err)
	}

	// Add credentials to the request (Execute uses BASIC Auth)
	auth := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", cfg.ExecuteKeyId, cfg.ExecuteKeySecret)))
	req.Header.Set("Authorization", "Basic "+auth)

	log.Debug("Pulling schema from Execute")
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("performing request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %v", err)
	}

	// Parse the retrieve document as JSON so that we can extract metadata fields
	var data RootSchema
	if err := json.Unmarshal(bodyBytes, &data); err != nil {
		return nil, fmt.Errorf("parsing schema: %v", err)
	}

	return data, nil
}
