# Nexus CLI Tool

This CLI submits events to the Nexus queue and checks Redis/MongoDB health.

## Build

```sh
go build -o nexus ./cmd/tool
```

## Usage

```sh
./nexus <command> [flags]
```

### Commands

- submit: Submit an event to the queue.
- health: Check Redis/MongoDB health and queue depth.

## Common flags

These flags are available on all commands:

- --redis-addr (default: localhost:6379)
- --redis-username
- --redis-password
- --redis-db (default: 0)
- --mongo-uri (default: mongodb://localhost:27017)
- --queue-key (default: nexus_queue)
- --timeout (default: 10s)

## Submit an event

Required:
- --delegation
- --payload or --payload-file

Optional:
- --max-attempts (default: 3)
- --priority (default: 0)
- --version (default: 1)

Example with a JSON payload file (Mandelbrot GIF):

```sh
./nexus submit \
  --delegation example.MandelbrotGenerator \
  --payload-file ./cmd/tool/payload.json
```

Example with an inline JSON payload (Mandelbrot PNG):

```sh
./nexus submit \
  --delegation example.MandelbrotGenerator \
  --payload '{"width":1920,"height":1080,"filename":"mandelbrot.png","format":"png","centerX":-0.5,"centerY":0,"zoom":1,"maxIter":256}'
```

Example map phase (word count):

```sh
./nexus submit \
  --delegation example.FileMapper \
  --payload '{"filenames":["mandelbrot.png"],"operation":"wordcount"}'
```

Example reduce phase (sum):

```sh
./nexus submit \
  --delegation example.FileReducer \
  --payload '{"filenames":["mandelbrot.png"],"operation":"sum"}'
```

Example upload (Cloudinary):

```sh
export CLOUDINARY_CLOUD_NAME=your_cloud_name
export CLOUDINARY_API_KEY=your_api_key
export CLOUDINARY_API_SECRET=your_api_secret

./nexus submit \
  --delegation example.FileUploader \
  --payload '{"filename":"mandelbrot.png"}'
```

## Health check

```sh
./nexus health
```

This prints Redis status, Mongo status, and the current queue length.
