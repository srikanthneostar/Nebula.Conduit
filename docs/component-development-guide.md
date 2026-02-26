# Component Development Guide

This guide explains how to build custom pipeline components for the Nebula Conduit data pipeline engine. The framework handles all the plumbing (channels, goroutines, context cancellation, retries) so you only write the business logic.

## Table of Contents

- [Overview](#overview)
- [Key Files Reference](#key-files-reference)
- [Architecture](#architecture)
- [Step-by-Step: Building a SQLite Reader (Source)](#step-by-step-building-a-sqlite-reader-source)
- [Step-by-Step: Building a SQLite Writer (Sink)](#step-by-step-building-a-sqlite-writer-sink)
- [Source Component Use Cases](#source-component-use-cases)
- [Processor Component Use Cases](#processor-component-use-cases)
- [Sink Component Use Cases](#sink-component-use-cases)
- [Registering Components](#registering-components)
- [Parameter Helpers](#parameter-helpers)
- [Data Helpers](#data-helpers)
- [Testing Components](#testing-components)
- [Advanced Patterns](#advanced-patterns)
- [API Reference](#api-reference)
- [Checklist](#checklist-adding-a-new-built-in-component)

---

## Overview

The pipeline engine moves data through a directed graph of components:

```
Source --> Processor --> Processor --> Sink
          (optional, any number)
```

Each component runs in its own goroutine. Data flows between them through Go channels as `pipeline.Data` structs. The framework provides three base structs:

| Category  | Base Struct              | You Implement                                      |
|-----------|--------------------------|----------------------------------------------------|
| Source    | `pipeline.BaseSource`    | `Generate(ctx, emit) error`                        |
| Processor | `pipeline.BaseProcessor` | `Transform(ctx, input Data) (Data, error)`         |
| Sink      | `pipeline.BaseSink`      | `Consume(ctx, input Data) error`                   |

Each base struct gives you `ID()`, `Type()`, `Config()`, `Validate()`, `Execute()`, channel management, and context cancellation for free.

---

## Key Files Reference

### Framework files (you use these, don't modify)

| File | Purpose |
|------|---------|
| `pkg/pipeline/component.go` | `Component`, `SourceComponent`, `ProcessorComponent`, `SinkComponent` interfaces |
| `pkg/pipeline/models.go` | `Data`, `ComponentConfig`, `ComponentType`, and all core data types |
| `pkg/pipeline/framework.go` | `BaseComponent`, `BaseSource`, `BaseProcessor`, `BaseSink` base structs |
| `pkg/pipeline/registry.go` | `DefaultRegistry` for `init()` self-registration |
| `pkg/pipeline/factory.go` | `ComponentFactory` interface and `ComponentConstructor` type |
| `pkg/pipeline/helpers.go` | Parameter helpers (`ParamString`, etc.) and data helpers (`DataToJSON`, etc.) |
| `pkg/pipeline/test_harness.go` | `ComponentTestHarness` for isolated testing |
| `pkg/pipeline/retry.go` | Retry logic wrapping your component automatically |
| `pkg/pipeline/executor.go` | Wires up channels between components and runs goroutines |
| `pkg/pipeline/graph.go` | DAG validation (source/sink presence, cycles, type compatibility) |

### Files you create or modify

| File | When |
|------|------|
| `pkg/pipeline/components/<name>.go` | Create this for your component implementation |
| `pkg/pipeline/components/<name>_test.go` | Create this for your component tests |
| `pkg/pipeline/models.go` | Add a `ComponentType` constant |
| `pkg/pipeline/components/register.go` | Register your component in `RegisterComponents()` |
| `pkg/pipeline/validation.go` | Add validation rules for your component config |
| `pkg/pipeline/graph.go` | Add your type to `isSourceComponent()` / `isSinkComponent()` / `isProcessorComponent()` |

### Existing components (use as reference)

| File | Component | Category |
|------|-----------|----------|
| `pkg/pipeline/components/csv.go` | CSV file reader | Source |
| `pkg/pipeline/components/http.go` | HTTP GET / HTTP POST | Source / Sink |
| `pkg/pipeline/components/sql.go` | SQL query executor | Source |
| `pkg/pipeline/components/tcp.go` | TCP read / TCP write | Source / Sink |
| `pkg/pipeline/components/kafka.go` | Kafka consumer / producer | Source / Sink |
| `pkg/pipeline/components/log.go` | Log passthrough | Processor |
| `pkg/pipeline/components/python.go` | Python code block | Processor |
| `pkg/pipeline/components/example_custom_component.go` | Minimal framework examples | All three |

---

## Architecture

### The Data Struct

> Defined in: `pkg/pipeline/models.go`

```go
type Data struct {
    Payload   interface{}       // The actual data
    Metadata  map[string]string // Key-value context
    Timestamp time.Time         // When created
    TraceID   string            // For tracing
}
```

### The ComponentConfig Struct

> Defined in: `pkg/pipeline/models.go`

```go
type ComponentConfig struct {
    ID              string                 `json:"id"`
    Type            ComponentType          `json:"type"`
    Parameters      map[string]interface{} `json:"parameters"`      // Your custom config
    RetryCount      int                    `json:"retry_count"`     // Handled by engine
    RetryDelay      time.Duration          `json:"retry_delay"`     // Handled by engine
    ContinueOnError bool                   `json:"continue_on_error"` // Handled by engine
    Timeout         time.Duration          `json:"timeout"`         // Handled by engine
}
```

Your custom configuration lives in `Parameters`. The engine handles the rest automatically via `pkg/pipeline/retry.go`.

---

## Step-by-Step: Building a SQLite Reader (Source)

This walkthrough builds a complete `sqlite_reader` source component from scratch. It reads rows from a SQLite table and emits each row as a `Data` item downstream.

### Step 1: Add the ComponentType constant

> File: `pkg/pipeline/models.go`

Open `pkg/pipeline/models.go` and add your type to the `ComponentType` constants block:

```go
const (
    // Source components
    ComponentTypeHTTPGet          ComponentType = "http_get"
    ComponentTypeSQLQuery         ComponentType = "sql_query"
    ComponentTypeCSVReader        ComponentType = "csv_reader"
    ComponentTypeSQLiteReader     ComponentType = "sqlite_reader"  // <-- add this
    // ... rest of existing constants
)
```

### Step 2: Create the component file

> Create: `pkg/pipeline/components/sqlite.go`

This is where your component logic lives. Create the file `pkg/pipeline/components/sqlite.go`:

```go
package components

import (
    "context"
    "database/sql"
    "encoding/json"
    "fmt"

    _ "github.com/mattn/go-sqlite3" // SQLite driver
    "github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
)

// NewSQLiteReaderComponent creates a new SQLite reader source component.
func NewSQLiteReaderComponent(config pipeline.ComponentConfig) (pipeline.Component, error) {
    // Step 2a: Extract parameters using helpers from pkg/pipeline/helpers.go
    dbPath, err := pipeline.ParamStringRequired(config.Parameters, "db_path")
    if err != nil {
        return nil, err
    }

    query, err := pipeline.ParamStringRequired(config.Parameters, "query")
    if err != nil {
        return nil, err
    }

    // Step 2b: Return a BaseSource (from pkg/pipeline/framework.go) with your Generate function
    return &pipeline.BaseSource{
        BaseComponent: pipeline.BaseComponent{Cfg: config},
        Generate: func(ctx context.Context, emit func(pipeline.Data) error) error {
            // Open the SQLite database
            db, err := sql.Open("sqlite3", dbPath)
            if err != nil {
                return fmt.Errorf("failed to open database %s: %w", dbPath, err)
            }
            defer db.Close()

            // Execute the query
            rows, err := db.QueryContext(ctx, query)
            if err != nil {
                return fmt.Errorf("query failed: %w", err)
            }
            defer rows.Close()

            // Get column names for building row maps
            columns, err := rows.Columns()
            if err != nil {
                return fmt.Errorf("failed to get columns: %w", err)
            }

            rowCount := 0
            for rows.Next() {
                // Create a slice of interface{} to hold each column value
                values := make([]interface{}, len(columns))
                valuePtrs := make([]interface{}, len(columns))
                for i := range values {
                    valuePtrs[i] = &values[i]
                }

                if err := rows.Scan(valuePtrs...); err != nil {
                    return fmt.Errorf("failed to scan row: %w", err)
                }

                // Build a map from column names to values
                rowMap := make(map[string]interface{}, len(columns))
                for i, col := range columns {
                    val := values[i]
                    // Convert []byte to string for readability
                    if b, ok := val.([]byte); ok {
                        val = string(b)
                    }
                    rowMap[col] = val
                }

                rowCount++

                // Emit each row as a Data item using helper from pkg/pipeline/helpers.go
                if err := emit(pipeline.NewData(rowMap, map[string]string{
                    "db_path":   dbPath,
                    "row_index": fmt.Sprintf("%d", rowCount),
                    "source":    "sqlite_reader",
                })); err != nil {
                    return err // Context cancelled, stop reading
                }
            }

            return rows.Err()
        },
    }, nil
}
```

### Step 3: Register the component

> File: `pkg/pipeline/components/register.go`

Open `pkg/pipeline/components/register.go` and add your component to the `RegisterComponents` function:

```go
func RegisterComponents(factory pipeline.ComponentFactory) {
    // ... existing registrations ...
    factory.Register(pipeline.ComponentTypeHTTPGet, NewHTTPGetComponent)
    factory.Register(pipeline.ComponentTypeSQLQuery, NewSQLQueryComponent)
    factory.Register(pipeline.ComponentTypeCSVReader, NewCSVReaderComponent)
    factory.Register(pipeline.ComponentTypeSQLiteReader, NewSQLiteReaderComponent) // <-- add this

    // ... rest of existing registrations ...
}
```

### Step 4: Add config validation

> File: `pkg/pipeline/validation.go`

Open `pkg/pipeline/validation.go` and do two things:

4a. Add a case to the `ValidateComponentConfig` switch (around line 92):

```go
    case ComponentTypeSQLiteReader:
        return validateSQLiteReaderConfig(config)
```

4b. Add the validation function (at the bottom of the file):

```go
func validateSQLiteReaderConfig(config ComponentConfig) error {
    if _, ok := config.Parameters["db_path"].(string); !ok {
        return fmt.Errorf("sqlite_reader requires 'db_path' parameter (string)")
    }
    if _, ok := config.Parameters["query"].(string); !ok {
        return fmt.Errorf("sqlite_reader requires 'query' parameter (string)")
    }
    return nil
}
```

### Step 5: Register in graph category

> File: `pkg/pipeline/graph.go`

Open `pkg/pipeline/graph.go` and add your type to the `isSourceComponent` function (around line 170):

```go
func isSourceComponent(compType ComponentType) bool {
    switch compType {
    case ComponentTypeHTTPGet,
        ComponentTypeSQLQuery,
        ComponentTypeCSVReader,
        ComponentTypeKafkaConsumer,
        ComponentTypeRabbitMQConsumer,
        ComponentTypeHL7Reader,
        ComponentTypeTCPRead,
        ComponentTypeSQLiteReader:  // <-- add this
        return true
    default:
        return false
    }
}
```

This tells the graph validator that your component is a source, so pipelines using it will pass the "must have at least one source" check.

### Step 6: Write tests

> Create: `pkg/pipeline/components/sqlite_test.go`

Use the `ComponentTestHarness` from `pkg/pipeline/test_harness.go`:

```go
package components

import (
    "context"
    "database/sql"
    "os"
    "testing"
    "time"

    _ "github.com/mattn/go-sqlite3"
    "github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
)

func TestSQLiteReaderComponent(t *testing.T) {
    // Create a temp SQLite database
    tmpFile := t.TempDir() + "/test.db"
    db, err := sql.Open("sqlite3", tmpFile)
    if err != nil {
        t.Fatal(err)
    }

    // Create table and insert test data
    db.Exec("CREATE TABLE users (id INTEGER, name TEXT, email TEXT)")
    db.Exec("INSERT INTO users VALUES (1, 'Alice', 'alice@test.com')")
    db.Exec("INSERT INTO users VALUES (2, 'Bob', 'bob@test.com')")
    db.Close()

    // Create the component
    comp, err := NewSQLiteReaderComponent(pipeline.ComponentConfig{
        ID:   "test-sqlite",
        Type: pipeline.ComponentTypeSQLiteReader,
        Parameters: map[string]interface{}{
            "db_path": tmpFile,
            "query":   "SELECT id, name, email FROM users ORDER BY id",
        },
    })
    if err != nil {
        t.Fatal(err)
    }

    // Run with test harness (from pkg/pipeline/test_harness.go)
    harness := pipeline.NewTestHarness(comp).WithTimeout(5 * time.Second)
    results, err := harness.RunSource(context.Background())
    if err != nil {
        t.Fatal(err)
    }

    // Verify results
    if len(results) != 2 {
        t.Fatalf("expected 2 rows, got %d", len(results))
    }

    // Check first row
    row, ok := results[0].Payload.(map[string]interface{})
    if !ok {
        t.Fatal("expected map payload")
    }
    if row["name"] != "Alice" {
        t.Errorf("expected Alice, got %v", row["name"])
    }
}
```

### Step 7: Verify it compiles

Run from the project root:

```bash
go build ./pkg/pipeline/...
```

### Summary of files touched

| Step | File | Action |
|------|------|--------|
| 1 | `pkg/pipeline/models.go` | Add `ComponentTypeSQLiteReader` constant |
| 2 | `pkg/pipeline/components/sqlite.go` | Create component with constructor |
| 3 | `pkg/pipeline/components/register.go` | Register in `RegisterComponents()` |
| 4 | `pkg/pipeline/validation.go` | Add validation case + function |
| 5 | `pkg/pipeline/graph.go` | Add to `isSourceComponent()` |
| 6 | `pkg/pipeline/components/sqlite_test.go` | Write tests |
| 7 | — | Build and verify |

### Pipeline config using this component

Once registered, you can use it in a pipeline definition like this:

```json
{
  "components": [
    {
      "id": "read-users",
      "type": "sqlite_reader",
      "parameters": {
        "db_path": "/data/app.db",
        "query": "SELECT * FROM users WHERE active = 1"
      },
      "retry_count": 2,
      "retry_delay": "5s"
    },
    {
      "id": "log-output",
      "type": "log",
      "parameters": { "log_level": "info", "format": "json" }
    },
    {
      "id": "send-to-api",
      "type": "http_post",
      "parameters": {
        "url": "https://api.example.com/users",
        "content_type": "application/json"
      }
    }
  ],
  "connections": [
    { "source_component_id": "read-users", "target_component_id": "log-output" },
    { "source_component_id": "log-output", "target_component_id": "send-to-api" }
  ]
}
```

Data flows: SQLite rows --> Log (inspect) --> HTTP POST (send to API)

---

## Step-by-Step: Building a SQLite Writer (Sink)

This walkthrough builds a `sqlite_writer` sink component that receives data from upstream components and inserts it into a SQLite table.

### Step 1: Add the ComponentType constant

> File: `pkg/pipeline/models.go`

```go
const (
    // Sink components
    ComponentTypeHTTPPost         ComponentType = "http_post"
    ComponentTypeSQLiteWriter     ComponentType = "sqlite_writer"  // <-- add this
    // ... rest of existing constants
)
```

### Step 2: Create the component file

> Create: `pkg/pipeline/components/sqlite.go` (add to the same file as the reader, or create a new one)

Add the sink constructor to `pkg/pipeline/components/sqlite.go`:

```go
// NewSQLiteWriterComponent creates a new SQLite writer sink component.
func NewSQLiteWriterComponent(config pipeline.ComponentConfig) (pipeline.Component, error) {
    // Extract parameters using helpers from pkg/pipeline/helpers.go
    dbPath, err := pipeline.ParamStringRequired(config.Parameters, "db_path")
    if err != nil {
        return nil, err
    }

    table, err := pipeline.ParamStringRequired(config.Parameters, "table")
    if err != nil {
        return nil, err
    }

    // Open the database once (connection reused across all Consume calls)
    db, err := sql.Open("sqlite3", dbPath)
    if err != nil {
        return nil, fmt.Errorf("failed to open database %s: %w", dbPath, err)
    }

    // Use a stateful sink (struct embedding BaseSink)
    sink := &sqliteWriterSink{db: db, table: table}
    sink.BaseComponent = pipeline.BaseComponent{Cfg: config}
    sink.Consume = func(ctx context.Context, input pipeline.Data) error {
        // Convert incoming Data payload to a map
        rowMap, ok := input.Payload.(map[string]interface{})
        if !ok {
            // Try converting from JSON bytes
            jsonBytes, err := pipeline.DataToBytes(input)
            if err != nil {
                return fmt.Errorf("cannot convert payload to map: %w", err)
            }
            if err := json.Unmarshal(jsonBytes, &rowMap); err != nil {
                return fmt.Errorf("payload is not a JSON object: %w", err)
            }
        }

        // Build INSERT statement dynamically from map keys
        columns := make([]string, 0, len(rowMap))
        placeholders := make([]string, 0, len(rowMap))
        values := make([]interface{}, 0, len(rowMap))
        for col, val := range rowMap {
            columns = append(columns, col)
            placeholders = append(placeholders, "?")
            values = append(values, val)
        }

        query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
            table,
            strings.Join(columns, ", "),
            strings.Join(placeholders, ", "),
        )

        _, err := db.ExecContext(ctx, query, values...)
        return err
    }

    return sink, nil
}

type sqliteWriterSink struct {
    pipeline.BaseSink
    db    *sql.DB
    table string
}

// Override Validate to check the database is accessible
func (s *sqliteWriterSink) Validate() error {
    return s.db.Ping()
}
```

Don't forget to add `"strings"` to the imports at the top of the file.

### Step 3: Register the component

> File: `pkg/pipeline/components/register.go`

```go
func RegisterComponents(factory pipeline.ComponentFactory) {
    // ... existing registrations ...
    factory.Register(pipeline.ComponentTypeSQLiteWriter, NewSQLiteWriterComponent) // <-- add this
}
```

### Step 4: Add config validation

> File: `pkg/pipeline/validation.go`

4a. Add a case to the switch:

```go
    case ComponentTypeSQLiteWriter:
        return validateSQLiteWriterConfig(config)
```

4b. Add the validation function:

```go
func validateSQLiteWriterConfig(config ComponentConfig) error {
    if _, ok := config.Parameters["db_path"].(string); !ok {
        return fmt.Errorf("sqlite_writer requires 'db_path' parameter (string)")
    }
    if _, ok := config.Parameters["table"].(string); !ok {
        return fmt.Errorf("sqlite_writer requires 'table' parameter (string)")
    }
    return nil
}
```

### Step 5: Register in graph category

> File: `pkg/pipeline/graph.go`

Add to the `isSinkComponent` function:

```go
func isSinkComponent(compType ComponentType) bool {
    switch compType {
    case ComponentTypeHTTPPost,
        ComponentTypeKafkaProducer,
        ComponentTypeRabbitMQProducer,
        ComponentTypeTCPWrite,
        ComponentTypeSQLiteWriter:  // <-- add this
        return true
    default:
        return false
    }
}
```

### Step 6: Write tests

> Create or add to: `pkg/pipeline/components/sqlite_test.go`

```go
func TestSQLiteWriterComponent(t *testing.T) {
    // Create a temp SQLite database with a table
    tmpFile := t.TempDir() + "/test_write.db"
    db, _ := sql.Open("sqlite3", tmpFile)
    db.Exec("CREATE TABLE events (id INTEGER, name TEXT, value REAL)")
    db.Close()

    // Create the sink component
    comp, err := NewSQLiteWriterComponent(pipeline.ComponentConfig{
        ID:   "test-sqlite-writer",
        Type: pipeline.ComponentTypeSQLiteWriter,
        Parameters: map[string]interface{}{
            "db_path": tmpFile,
            "table":   "events",
        },
    })
    if err != nil {
        t.Fatal(err)
    }

    // Use test harness (from pkg/pipeline/test_harness.go)
    harness := pipeline.NewTestHarness(comp)
    harness.SendInput(pipeline.NewData(map[string]interface{}{
        "id": 1, "name": "temperature", "value": 23.5,
    }, nil))
    harness.SendInput(pipeline.NewData(map[string]interface{}{
        "id": 2, "name": "humidity", "value": 65.0,
    }, nil))
    harness.CloseInput()

    _, err = harness.Run(context.Background())
    if err != nil {
        t.Fatal(err)
    }

    // Verify data was written
    db, _ = sql.Open("sqlite3", tmpFile)
    defer db.Close()
    var count int
    db.QueryRow("SELECT COUNT(*) FROM events").Scan(&count)
    if count != 2 {
        t.Errorf("expected 2 rows, got %d", count)
    }
}
```

### Summary of files touched

| Step | File | Action |
|------|------|--------|
| 1 | `pkg/pipeline/models.go` | Add `ComponentTypeSQLiteWriter` constant |
| 2 | `pkg/pipeline/components/sqlite.go` | Add sink constructor |
| 3 | `pkg/pipeline/components/register.go` | Register in `RegisterComponents()` |
| 4 | `pkg/pipeline/validation.go` | Add validation case + function |
| 5 | `pkg/pipeline/graph.go` | Add to `isSinkComponent()` |
| 6 | `pkg/pipeline/components/sqlite_test.go` | Write tests |

### Pipeline config: SQLite-to-SQLite with transformation

```json
{
  "components": [
    {
      "id": "read-raw",
      "type": "sqlite_reader",
      "parameters": {
        "db_path": "/data/raw.db",
        "query": "SELECT * FROM sensor_data WHERE processed = 0"
      }
    },
    {
      "id": "transform",
      "type": "python_code_block",
      "parameters": {
        "code": "import json\ndata = json.loads(input_data)\ndata['celsius'] = (data['fahrenheit'] - 32) * 5/9\noutput_data = json.dumps(data)"
      },
      "timeout": "30s"
    },
    {
      "id": "write-processed",
      "type": "sqlite_writer",
      "parameters": {
        "db_path": "/data/processed.db",
        "table": "converted_readings"
      }
    }
  ],
  "connections": [
    { "source_component_id": "read-raw", "target_component_id": "transform" },
    { "source_component_id": "transform", "target_component_id": "write-processed" }
  ]
}
```

Data flows: SQLite (raw) --> Python (convert F to C) --> SQLite (processed)

---

## Source Component Use Cases

Sources produce data. Here are practical use cases with the component type, what it does, and an example pipeline it would fit into.

### 1. File System Reader
Read files from a directory (JSON, XML, text). Emit each file or each line as a Data item.

```
[File Reader] --> [JSON Parser Processor] --> [HTTP POST Sink]
```
Use case: Watch a drop folder for incoming data files and forward parsed contents to an API.

### 2. REST API Poller
Poll a REST API on an interval. Emit each response as a Data item.

```
[API Poller] --> [Log] --> [Kafka Producer Sink]
```
Use case: Periodically fetch exchange rates from an API and publish to a Kafka topic.

### 3. SQLite / Database Reader
Query a database table. Emit each row as a Data item (as shown in the walkthrough above).

```
[SQLite Reader] --> [Python Transform] --> [CSV Writer Sink]
```
Use case: Export database records to CSV after applying transformations.

### 4. MQTT Subscriber
Subscribe to an MQTT topic. Emit each message as a Data item.

```
[MQTT Subscriber] --> [JSON Filter Processor] --> [SQLite Writer Sink]
```
Use case: Collect IoT sensor data from MQTT and store filtered readings in a database.

### 5. S3 / Cloud Storage Reader
List and read objects from an S3 bucket. Emit each object's contents as a Data item.

```
[S3 Reader] --> [CSV Parser Processor] --> [SQL Insert Sink]
```
Use case: Process data files uploaded to cloud storage.

### 6. Webhook Listener
Listen on an HTTP endpoint for incoming POST requests. Emit each request body as a Data item.

```
[Webhook Listener] --> [Validator Processor] --> [RabbitMQ Producer Sink]
```
Use case: Receive webhook events from a third-party service and queue them for processing.

### 7. Log File Tailer
Tail a log file (like `tail -f`). Emit each new line as a Data item.

```
[Log Tailer] --> [Regex Parser Processor] --> [Elasticsearch Sink]
```
Use case: Stream application logs into a search engine in real time.

---

## Processor Component Use Cases

Processors transform data one item at a time. They sit between sources and sinks.

### 1. JSON Field Filter
Keep only records where a specific field matches a value. Return error for non-matching items with `ContinueOnError: true` to skip them.

```
[HTTP GET Source] --> [JSON Filter] --> [Kafka Sink]
```

### 2. Data Enrichment
Look up additional data (e.g. from a cache or API) and merge it into the record.

```
[SQLite Reader] --> [Enrichment Processor] --> [HTTP POST Sink]
```

### 3. Format Converter
Convert between formats: JSON to XML, CSV row to JSON, bytes to string, etc.

```
[CSV Reader] --> [CSV-to-JSON Converter] --> [Kafka Sink]
```

### 4. Aggregator / Batch Collector
Collect N items into a batch array, then emit the batch as a single Data item.

```
[MQTT Source] --> [Batch Collector (100 items)] --> [HTTP POST Sink]
```

### 5. Deduplicator
Track seen IDs in a map. Skip items that have already been processed.

```
[Kafka Consumer] --> [Deduplicator] --> [SQLite Writer]
```

---

## Sink Component Use Cases

Sinks consume data. They are the terminal nodes of a pipeline.

### 1. Database Writer
Insert each incoming Data item as a row in a database table (as shown in the SQLite Writer walkthrough).

```
[HTTP GET Source] --> [Transform] --> [SQLite Writer Sink]
```
Use case: Fetch data from an API and store it locally.

### 2. File Writer
Append each Data item as a line to a file (JSON lines, CSV, plain text).

```
[Kafka Consumer] --> [JSON Formatter] --> [File Writer Sink]
```
Use case: Archive Kafka messages to disk.

### 3. REST API Sender
POST each Data item to a REST API endpoint (already built-in as `http_post`).

```
[SQLite Reader] --> [API Sender Sink]
```
Use case: Sync local database records to a remote service.

### 4. Email / Notification Sender
Send an email or Slack/Teams notification for each Data item.

```
[Log Tailer Source] --> [Error Filter Processor] --> [Slack Notification Sink]
```
Use case: Alert on application errors detected in log files.

### 5. S3 / Cloud Storage Writer
Upload each Data item as an object to S3 or cloud storage.

```
[Database Reader] --> [JSON Formatter] --> [S3 Writer Sink]
```
Use case: Export database snapshots to cloud storage for archival.

### 6. Metrics Emitter
Push each Data item as a metric to Prometheus, StatsD, or a monitoring system.

```
[MQTT Source] --> [Metrics Emitter Sink]
```
Use case: Forward IoT sensor readings to a monitoring dashboard.

---

## Registering Components

There are three ways to make your component available to the pipeline engine.

### Option 1: Self-registration via init() (recommended)

> Registry defined in: `pkg/pipeline/registry.go`

Add an `init()` function in your component file (e.g. `pkg/pipeline/components/sqlite.go`):

```go
func init() {
    pipeline.DefaultRegistry.Register("sqlite_reader", NewSQLiteReaderComponent)
    pipeline.DefaultRegistry.Register("sqlite_writer", NewSQLiteWriterComponent)
}
```

Then in your main application (e.g. `cmd/server/main.go`), import the package and call `RegisterAll`:

```go
import (
    _ "github.com/Xecutables/Nebula.Conduit/pkg/pipeline/components" // triggers init()
    "github.com/Xecutables/Nebula.Conduit/pkg/pipeline"
)

func main() {
    factory := pipeline.NewComponentFactory()
    pipeline.DefaultRegistry.RegisterAll(factory) // loads all init()-registered components
}
```

### Option 2: Direct factory registration

> Factory defined in: `pkg/pipeline/factory.go`

```go
factory := pipeline.NewComponentFactory()
factory.Register("sqlite_reader", NewSQLiteReaderComponent)
```

### Option 3: Add to the built-in register.go

> File: `pkg/pipeline/components/register.go`

```go
func RegisterComponents(factory pipeline.ComponentFactory) {
    // ... existing registrations ...
    factory.Register(pipeline.ComponentTypeSQLiteReader, NewSQLiteReaderComponent)
    factory.Register(pipeline.ComponentTypeSQLiteWriter, NewSQLiteWriterComponent)
}
```

---

## Parameter Helpers

> Defined in: `pkg/pipeline/helpers.go`

Type-safe extraction from `ComponentConfig.Parameters` with JSON coercion:

```go
// Strings
val, err := pipeline.ParamString(params, "key", "default")
val, err := pipeline.ParamStringRequired(params, "key")

// Numbers (JSON float64 -> int conversion handled)
val, err := pipeline.ParamInt(params, "key", 10)
val, err := pipeline.ParamFloat(params, "key", 3.14)

// Booleans
val, err := pipeline.ParamBool(params, "key", false)

// Slices and maps (JSON []interface{} -> []string conversion handled)
val, err := pipeline.ParamStringSlice(params, "key", []string{"a", "b"})
val, err := pipeline.ParamMap(params, "key", nil)

// Durations (from strings like "30s", "5m")
val, err := pipeline.ParamDuration(params, "key", 30*time.Second)
```

---

## Data Helpers

> Defined in: `pkg/pipeline/helpers.go`

```go
// Payload -> JSON bytes
jsonBytes, err := pipeline.DataToJSON(data)

// JSON bytes -> Data
data, err := pipeline.DataFromJSON(jsonBytes)

// Payload -> []byte
raw, err := pipeline.DataToBytes(data)

// []byte -> Data
data := pipeline.DataFromBytes(raw)

// Payload -> string
str, err := pipeline.DataToString(data)

// Convenience constructor
data := pipeline.NewData("hello", map[string]string{"source": "test"})
```

---

## Testing Components

> Test harness defined in: `pkg/pipeline/test_harness.go`
>
> Existing test examples: `pkg/pipeline/components/http_test.go`, `pkg/pipeline/components/log_test.go`

### Testing a Source

```go
harness := pipeline.NewTestHarness(comp).WithTimeout(5 * time.Second)
results, err := harness.RunSource(context.Background())
// results is []pipeline.Data with all emitted items
```

### Testing a Processor

```go
harness := pipeline.NewTestHarness(comp)
harness.SendInput(pipeline.NewData("input-value", nil))
harness.CloseInput()
results, err := harness.Run(context.Background())
// results is []pipeline.Data with all transformed items
```

### Testing a Sink

```go
harness := pipeline.NewTestHarness(comp)
harness.SendInput(pipeline.NewData("write-me", nil))
harness.CloseInput()
_, err := harness.Run(context.Background())
// err is nil on success; verify side effects (file written, DB row inserted, etc.)
```

Place test files alongside your component: `pkg/pipeline/components/<name>_test.go`

---

## Advanced Patterns

### Custom Validate()

Override `Validate()` on your own struct. The executor calls it (in `pkg/pipeline/executor.go`) before running each component.

```go
type MySource struct {
    pipeline.BaseSource
    filePath string
}

func (s *MySource) Validate() error {
    if _, err := os.Stat(s.filePath); os.IsNotExist(err) {
        return fmt.Errorf("file does not exist: %s", s.filePath)
    }
    return nil
}
```

### Stateful Sink (connection pooling)

Embed `BaseSink` in a struct that holds a DB connection, file handle, or client:

```go
type DBSink struct {
    pipeline.BaseSink
    db *sql.DB
}
```

See the SQLite Writer walkthrough above for a complete example.

### Polling Source (periodic data generation)

Use a `time.Ticker` inside `Generate` to emit data on an interval:

```go
Generate: func(ctx context.Context, emit func(pipeline.Data) error) error {
    ticker := time.NewTicker(interval)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done():
            return nil
        case <-ticker.C:
            // fetch data and emit
        }
    }
}
```

---

## API Reference

### Base Structs

> All defined in: `pkg/pipeline/framework.go`

| Struct | Embeds | You Set | Satisfies |
|--------|--------|---------|-----------|
| `BaseComponent` | -- | `Cfg` | `ID()`, `Type()`, `Config()`, `Validate()` |
| `BaseSource` | `BaseComponent` | `Generate` | `Component`, `SourceComponent` |
| `BaseProcessor` | `BaseComponent` | `Transform` | `Component`, `ProcessorComponent` |
| `BaseSink` | `BaseComponent` | `Consume` | `Component`, `SinkComponent` |

### Function Signatures

> `pkg/pipeline/framework.go` and `pkg/pipeline/factory.go`

```go
type SourceFunc    func(ctx context.Context, emit func(Data) error) error
type TransformFunc func(ctx context.Context, input Data) (Data, error)
type ConsumeFunc   func(ctx context.Context, input Data) error
type ComponentConstructor func(config ComponentConfig) (Component, error)
```

### Registry

> `pkg/pipeline/registry.go`

```go
pipeline.DefaultRegistry.Register("my_type", NewMyComponent)  // in init()
pipeline.DefaultRegistry.RegisterAll(factory)                  // in main/setup
```

### Test Harness

> `pkg/pipeline/test_harness.go`

```go
harness := pipeline.NewTestHarness(component)
harness.WithTimeout(5 * time.Second)
harness.SendInput(data)
harness.CloseInput()
results, err := harness.Run(ctx)
results, err := harness.RunSource(ctx)
```

---

## Checklist: Adding a New Built-in Component

| Step | File | What to do |
|------|------|------------|
| 1 | `pkg/pipeline/models.go` | Add a `ComponentType` constant |
| 2 | `pkg/pipeline/components/<name>.go` | Create component file with constructor using `BaseSource` / `BaseProcessor` / `BaseSink` |
| 3 | `pkg/pipeline/components/register.go` | Add `factory.Register(...)` line in `RegisterComponents()` |
| 4a | `pkg/pipeline/validation.go` | Add `case` in `ValidateComponentConfig` switch |
| 4b | `pkg/pipeline/validation.go` | Add `validate<Name>Config()` function |
| 5 | `pkg/pipeline/graph.go` | Add type to `isSourceComponent()` / `isSinkComponent()` / `isProcessorComponent()` |
| 6 | `pkg/pipeline/components/<name>_test.go` | Write tests using `ComponentTestHarness` |
| 7 | Terminal | Run `go build ./pkg/pipeline/...` to verify |
