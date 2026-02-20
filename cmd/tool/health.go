package main

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/spf13/cobra"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func newHealthCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "health",
		Short: "Check Redis/MongoDB health and queue depth",
		RunE:  runHealth,
	}
}

func runHealth(cmd *cobra.Command, _ []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()

	redisClient := newRedisClient()
	defer redisClient.Close()

	mongoClient, err := newMongoClient(ctx)
	if err != nil {
		printJSON("mongo", "down")
		return err
	}
	defer mongoClient.Disconnect(ctx)

	redisState, queueLength := redisHealth(ctx, redisClient)
	mongoState := "running"
	if err := mongoClient.Ping(ctx, readpref.Nearest()); err != nil {
		mongoState = "down"
	}

	printJSON("redis", redisState)
	printJSON("mongo", mongoState)
	printJSON("queue_length", queueLength)

	if redisState != "running" || mongoState != "running" {
		return fmt.Errorf("one or more dependencies are down")
	}
	return nil
}

func redisHealth(ctx context.Context, client *redis.Client) (string, int64) {
	if err := client.Ping(ctx).Err(); err != nil {
		return "down", -1
	}
	length, err := client.LLen(ctx, queueKey).Result()
	if err != nil {
		return "running", -1
	}
	return "running", length
}
