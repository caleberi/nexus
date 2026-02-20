# Quickstart Example

Demonstrates a complete Nexus event processing pipeline with three sample plugins and a NexusCore orchestrator.

## What it does

1. **Connects** to Redis (queue + cache) and MongoDB (event persistence)
2. **Registers** three plugins:
   - `example.ImageGenerator` – creates a PNG with random colors
   - `example.TextGenerator` – writes a random text file
   - `example.TextProcessor` – reads a text file and counts words
3. **Submits** three events (one for each plugin) to the queue
4. **Processes** events asynchronously using NexusCore workers
5. **Streams** task state updates to stdout as events complete

## Running locally

### Prerequisites
- Redis running on `localhost:6379`
- MongoDB running on `localhost:27017`

### Run
```bash
cd example/quickstart
go run main.go
```

Output files are written to `./generated/`.

## Running with Docker Compose

From the project root:

```bash
docker compose up redis mongodb
docker compose --profile demo up
```

The `demo` service waits for Redis and MongoDB health checks before starting.

## How it works

### Plugin registration
Each plugin implements the `nexus.Plugin` interface with:
- `Meta()` – returns plugin name, description, version, and JSON schema
- `Execute(ctx, args)` – performs the plugin's work

### Event submission
Events are created with:
- `DelegationType` – plugin name (e.g., `example.TextGenerator`)
- `Payload` – JSON arguments matching the plugin's schema
- `MaxAttempts` – retry limit before dead-lettering

### Event processing
1. NexusCore pulls events from Redis
2. Routes each event to the matching plugin
3. Plugin executes with the provided arguments
4. Result is stored in MongoDB
5. Task state update is pushed to the task queue

### Task state streaming
A goroutine polls `taskQueue.PullNewTaskState()` and prints updates as JSON, showing:
- Event ID
- Status (PENDING, STARTED, COMPLETED, FAILED)
- Results or error messages
- Timestamps

## Customization

Edit payloads in `main()`:
```go
imagePayload := `{"width": 800, "height": 600, "filename": "custom.png", "seed": 12345}`
```

Or use the CLI to submit events after starting the quickstart:
```bash
nexus submit --delegation example.TextGenerator --payload-file payload.json
```
