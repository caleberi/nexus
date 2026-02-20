package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"grain/nexus"

	"github.com/cenkalti/backoff/v4"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const outputDir = "./generated"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	redisAddr := getEnv("NEXUS_REDIS_ADDR", "localhost:6379")
	mongoURI := getEnv("NEXUS_MONGO_URI", "mongodb://localhost:27017")

	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	redisCache := &nexus.RedisCache{InternalCache: redisClient}
	taskQueue := nexus.NewRedisTaskQueue(redisCache, "task_state_queue")

	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to connect to MongoDB")
	}
	defer mongoClient.Disconnect(ctx)

	plugins := map[string]nexus.Plugin{
		"example.MandelbrotGenerator": &MandelbrotGeneratorPlugin{},
		"example.FileMapper":          &FileMapPlugin{},
		"example.FileReducer":         &FileReducePlugin{},
		"example.FileUploader":        &FileUploadPlugin{},
	}

	core, err := nexus.NewNexusCore(ctx, nexus.NexusCoreBackendArgs{
		Redis:                           nexus.RedisArgs{Url: redisAddr, Db: 0},
		MongoDbClient:                   mongoClient,
		Plugins:                         plugins,
		Logger:                          logger,
		MaxPluginWorkers:                2,
		TaskStateQueue:                  taskQueue,
		MaxFlowQueueLength:              1000,
		ScanAndFixFlowInterval:          2 * time.Second,
		StreamCapacity:                  1000,
		MaxConcurrentPluginPerExecution: 1,
	})
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to initialize NexusCore")
	}

	go core.Run(2)

	go streamTaskState(ctx, logger, taskQueue)

	// Generate Mandelbrot fractal (GIF with zoom animation)
	mandelbrotFilename := "mandelbrot_zoom.gif"
	mandelbrotPayload := fmt.Sprintf(`{
		"width": 800,
		"height": 600,
		"filename": %q,
		"format": "gif",
		"centerX": -0.7,
		"centerY": 0.0,
		"zoom": 1,
		"maxIter": 256,
		"frames": 30
	}`, mandelbrotFilename)

	if err := core.SubmitEvent(nexus.EventDetail{
		DelegationType: "example.MandelbrotGenerator",
		Payload:        mandelbrotPayload,
		MaxAttempts:    3,
	}, backoff.NewExponentialBackOff()); err != nil {
		logger.Fatal().Err(err).Msg("failed to submit mandelbrot event")
	}

	// Generate static Mandelbrot fractal (PNG)
	mandelbrotPNG := "mandelbrot.png"
	mandelbrotPNGPayload := fmt.Sprintf(`{
		"width": 1920,
		"height": 1080,
		"filename": %q,
		"format": "png",
		"centerX": -0.5,
		"centerY": 0.0,
		"zoom": 1,
		"maxIter": 256
	}`, mandelbrotPNG)

	if err := core.SubmitEvent(nexus.EventDetail{
		DelegationType: "example.MandelbrotGenerator",
		Payload:        mandelbrotPNGPayload,
		MaxAttempts:    3,
	}, backoff.NewExponentialBackOff()); err != nil {
		logger.Fatal().Err(err).Msg("failed to submit mandelbrot PNG event")
	}

	// Upload the generated fractals
	uploadPayload := fmt.Sprintf(`{"filename": %q}`, mandelbrotFilename)
	if err := core.SubmitEvent(nexus.EventDetail{
		DelegationType: "example.FileUploader",
		Payload:        uploadPayload,
		MaxAttempts:    3,
	}, backoff.NewExponentialBackOff()); err != nil {
		logger.Fatal().Err(err).Msg("failed to submit upload event")
	}

	<-ctx.Done()
	core.Shutdown()
}

func streamTaskState(ctx context.Context, logger zerolog.Logger, queue nexus.TaskStateQueue) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			pollCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			state, err := queue.PullNewTaskState(pollCtx)
			cancel()
			if err != nil {
				logger.Error().Err(err).Msg("failed to pull task state")
				time.Sleep(200 * time.Millisecond)
				continue
			}
			if state == nil {
				time.Sleep(200 * time.Millisecond)
				continue
			}
			data, err := json.MarshalIndent(state, "", "  ")
			if err != nil {
				logger.Error().Err(err).Msg("failed to marshal task state")
				continue
			}
			fmt.Printf("task_state: %s\n", string(data))
		}
	}
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func ensureOutputDir() error {
	return os.MkdirAll(outputDir, 0755)
}
