package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"grain/nexus"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	redisAddr     string
	redisUsername string
	redisPassword string
	redisDB       int
	mongoURI      string
	queueKey      string
	opTimeout     time.Duration
)

var rootCmd = &cobra.Command{
	Use:   "nexus",
	Short: "Nexus CLI for submitting events and checking queue health",
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&redisAddr, "redis-addr", "localhost:6379", "Redis address")
	rootCmd.PersistentFlags().StringVar(&redisUsername, "redis-username", "", "Redis username")
	rootCmd.PersistentFlags().StringVar(&redisPassword, "redis-password", "", "Redis password")
	rootCmd.PersistentFlags().IntVar(&redisDB, "redis-db", 0, "Redis database")
	rootCmd.PersistentFlags().StringVar(&mongoURI, "mongo-uri", "mongodb://localhost:27017", "MongoDB connection URI")
	rootCmd.PersistentFlags().StringVar(&queueKey, "queue-key", "nexus_queue", "Redis queue key")
	rootCmd.PersistentFlags().DurationVar(&opTimeout, "timeout", 10*time.Second, "Operation timeout")

	rootCmd.AddCommand(newSubmitCmd())
	rootCmd.AddCommand(newHealthCmd())
}

func newLogger() zerolog.Logger {
	return zerolog.New(os.Stdout).With().Timestamp().Logger()
}

func newRedisClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Username: redisUsername,
		Password: redisPassword,
		DB:       redisDB,
	})
}

func newMongoClient(ctx context.Context) (*mongo.Client, error) {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return nil, err
	}
	return client, nil
}

func newFlow(ctx context.Context, logger zerolog.Logger, redisClient *redis.Client, mongoClient *mongo.Client) (*nexus.NexusFlow, error) {
	return nexus.NewNexusFlow(
		ctx,
		nexus.NexusFlowArgs{
			Key:              queueKey,
			Client:           redisClient,
			Logger:           logger,
			DbClient:         mongoClient,
			OperationTimeout: opTimeout,
		})
}

func printJSON(label string, value any) {
	fmt.Printf("%s: %v\n", label, value)
}
