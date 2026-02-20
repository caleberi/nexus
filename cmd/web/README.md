# Nexus Web UI

Web interface for creating, compiling, and testing Grain plugins.

## Setup

### Backend

1. Install Fiber dependency:
```sh
go get github.com/gofiber/fiber/v2
```

2. Build and run:
```sh
cd cmd/web
go run main.go
```

Server runs on `http://localhost:3000` by default. Configure with env vars:
- `NEXUS_WEB_PORT` (default: 3000)
- `NEXUS_REDIS_ADDR` (default: localhost:6379)
- `NEXUS_MONGO_URI` (default: mongodb://localhost:27017)

### Frontend

1. Install dependencies:
```sh
cd cmd/web/frontend
npm install
```

2. Development mode:
```sh
npm run dev
```

3. Build for production:
```sh
npm run build
```

## Usage

1. **Create Plugin**: Write Go code in the editor and click "Create Plugin"
2. **Compile**: Select plugin and click "Compile" to generate `.so` file
3. **Submit Event**: Fill JSON payload and submit directly to Nexus core

Your plugins must implement the `nexus.Plugin` interface:
```go
type Plugin interface {
    Meta() PluginMeta
    Execute(ctx context.Context, args <YourArgsType>) (string, error)
}
```

## Features

- Monaco editor with Go syntax highlighting
- Real-time plugin compilation
- Plugin list with compilation status
- Event submission to Nexus core
- Redis/MongoDB health check
- Live status updates
