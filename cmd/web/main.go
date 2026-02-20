package main

import (
	"os"

	server "grain/cmd/web/backend"

	"github.com/rs/zerolog"
)

func main() {
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	port := os.Getenv("NEXUS_WEB_PORT")
	if port == "" {
		port = "3000"
	}

	redisAddr := os.Getenv("NEXUS_REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	mongoURI := os.Getenv("NEXUS_MONGO_URI")
	if mongoURI == "" {
		mongoURI = "mongodb://localhost:27017"
	}

	app, err := server.NewNexusWebServer(
		server.Config{
			Port:      port,
			RedisAddr: redisAddr,
			MongoURI:  mongoURI,
		}, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to create app")
	}

	if err := app.Start(); err != nil {
		logger.Fatal().Err(err).Msg("Failed to start app")
	}
}
