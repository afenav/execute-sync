# Snowflake Error Handling and Logging Improvements

This document summarizes the comprehensive improvements made to the Snowflake sync functionality in `src/internal/warehouses/snowflake/snowflake.go`.

## Key Improvements Implemented

### 1. JSON Size Validation and Warnings
```go
const (
    DefaultMaxJSONSize int = 10 * 1024 * 1024 // 10MB - Snowflake VARIANT recommended limit
    WarningJSONSize    int = 8 * 1024 * 1024  // 8MB - warn at 80% of limit
    ExtremeJSONSize    int = 15 * 1024 * 1024 // 15MB - fail fast on extremely large objects
)
```
- **8MB Warning Threshold**: Logs warnings for large JSON objects that may impact performance
- **10MB Recommended Limit**: Aligns with Snowflake VARIANT best practices
- **15MB Extreme Limit**: Fails fast on objects that would likely cause Snowflake issues

### 2. Enhanced CSV Error Handling
```go
type UploadStats struct {
    DocumentsProcessed  int
    ChunksWritten       int
    ChunksFailedToWrite int
    LargeJSONWarnings   int
    ExtremeJSONFailures int
    StartTime           time.Time
    CSVWriteErrors      []string
}
```
- Tracks success/failure ratios for all operations
- Continues processing on individual failures instead of stopping entire batch
- Collects detailed error information for debugging

### 3. Improved Snowflake Pipeline Monitoring
- **Stage Upload Monitoring**: Times and validates file uploads to Snowflake stage
- **Pipe Refresh Monitoring**: Tracks Snowpipe refresh operations with detailed logging
- **Empty Batch Handling**: Explicit detection and logging of empty batches

### 4. Better Error Context and Field Validation
```go
func validateRequiredFields(data map[string]interface{}) error {
    requiredFields := []string{"$TYPE", "DOCUMENT_ID", "$VERSION", "$AUTHOR_ID", "$DATE", "$DELETED"}
    // ... validation logic
}
```
- Validates all required fields before processing
- Provides detailed error context with document IDs, chunk indices, and batch information
- Fixed previously ignored JSON marshaling errors

### 5. Structured Logging Enhancements
```go
log.Info("Upload progress", 
    "documents_processed", stats.DocumentsProcessed,
    "chunks_written", stats.ChunksWritten,
    "chunks_failed", stats.ChunksFailedToWrite,
    "elapsed_seconds", int(elapsed.Seconds()),
    "batch_date", batch_date)
```
- Key-value structured logging for better searchability
- Progress logging for large batches (every 1000 documents)
- Comprehensive timing information for performance monitoring
- Detailed statistics at completion

## Benefits

### 1. Clear Visibility
- **Record Loss Detection**: Detailed tracking shows exactly where records might be getting lost
- **Progress Monitoring**: Real-time visibility into processing status
- **Performance Metrics**: Timing information helps identify bottlenecks

### 2. Better Diagnostics
- **Rich Error Context**: Every error includes document ID, chunk index, batch date, and operation context
- **Size Warnings**: Proactive identification of objects that may cause issues
- **Failure Categorization**: Different types of failures are tracked and reported separately

### 3. Production Readiness
- **Non-Disruptive**: Continues processing when individual records fail
- **Performance Conscious**: Efficient logging that doesn't impact sync performance
- **Backward Compatible**: Maintains existing API and behavior

## Sample Log Output

```
INFO Starting Snowflake upload batch_date=2024-08-23T01:00:00Z chunk_size=10000
DEBUG Processing document document_id=doc123 type=ExecuteDocument
WARN Large JSON object detected, may impact Snowflake performance document_id=doc123 chunk_index=0 size_mb=9.2
INFO Upload progress documents_processed=1000 chunks_written=1998 chunks_failed=2 elapsed_seconds=45
INFO Uploading CSV to Snowflake Stage file_path=/tmp/documents_20240823*.csv batch_date=2024-08-23T01:00:00Z
INFO Snowflake upload completed successfully batch_date=2024-08-23T01:00:00Z total_time_seconds=67 success_rate_percent=99.9
```

## Configuration

Size limits are defined as constants for easy adjustment:
- Modify the constants at the top of the file to adjust thresholds
- Future enhancement could make these configurable via CLI flags or environment variables

## Backward Compatibility

All changes maintain full backward compatibility:
- Same function signatures and return values
- Same behavior for successful operations
- Enhanced error handling and logging are additive improvements