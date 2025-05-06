package execute

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
