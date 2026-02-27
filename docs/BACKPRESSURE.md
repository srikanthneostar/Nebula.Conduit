# Backpressure Handling with MongoDB Queue

## Problem
When a component (like CSV reader) runs on a schedule (e.g., every 5 minutes) but the downstream component (like log sink) is still processing from the previous run, data can accumulate and overwhelm the system.

## Solution
The system now supports optional MongoDB-based queuing for backpressure handling. When enabled:

1. **Fast producers** (CSV reader) write data to MongoDB queue
2. **Slow consumers** (log sink) read from the queue at their own pace
3. **No data loss** - all data is persisted in MongoDB
4. **Automatic scaling** - queue handles the backlog

## Configuration

### Enable MongoDB in config.yaml

```yaml
mongodb:
  enabled: true
  connection_string: "mongodb://localhost:27017"
  database_name: "nebula_conduit"
  username: "your_username"
  password: "your_password"
```

### Disable MongoDB (default - no queuing)

```yaml
mongodb:
  enabled: false
```

## How It Works

### Without MongoDB (Default)
```
CSV Reader → Log Sink
     ↓
If Log Sink is slow, data backs up in memory
```

### With MongoDB
```
CSV Reader → MongoDB Queue → Log Sink
     ↓
If Log Sink is slow, data goes to MongoDB queue
```

## Queue Per Component

Each component gets its own MongoDB collection:
- `queue_csv-reader-1`
- `queue_log-sink-1`
- `queue_attr-update-1`

## Benefits

1. **No data loss** - All data is persisted
2. **No memory overflow** - Queue handles large backlogs
3. **Decoupled processing** - Components work at their own pace
4. **Recoverable** - Queue data survives restarts

## Limitations

1. **Requires MongoDB** - Need to run MongoDB server
2. **Network overhead** - Extra network calls to MongoDB
3. **Storage cost** - Queue data uses disk space

## When to Use

### Enable MongoDB Queue When:
- Processing large CSV files
- Components have different processing speeds
- Data cannot be lost
- Long-running operations

### Disable MongoDB Queue When:
- Simple pipelines with fast components
- Development/testing
- No MongoDB available
- Low-volume processing

## Monitoring

Check queue sizes via metrics endpoint:
```
GET /metrics
```

Look for `backpressure.task_queue_size` in the response.

## Example Configuration

### Full Pipeline with Backpressure

```yaml
server:
  host: "0.0.0.0"
  port: 8080

mongodb:
  enabled: true
  connection_string: "mongodb://localhost:27017"
  database_name: "nebula_conduit"

database:
  path: "Nebula.Conduit.db"
```

### Simple Pipeline (No Backpressure)

```yaml
server:
  host: "0.0.0.0"
  port: 8080

mongodb:
  enabled: false

database:
  path: "Nebula.Conduit.db"
```

## Migration Guide

### From No Backpressure to MongoDB

1. Install MongoDB
2. Update `config.yaml` with MongoDB settings
3. Restart server
4. Existing pipelines will automatically use the queue

### From MongoDB to No Backpressure

1. Update `config.yaml` to disable MongoDB
2. Restart server
3. Queue data is preserved (can be cleared manually if needed)

## Troubleshooting

### MongoDB Connection Failed

Check:
1. MongoDB is running
2. Connection string is correct
3. Network connectivity

### Queue Growing Too Large

Check:
1. Downstream component is processing
2. No errors in logs
3. Consider increasing component resources

### Clear Queue

```bash
# Connect to MongoDB and drop queue collection
mongo nebula_conduit
db.queue_component-id.drop()
```
