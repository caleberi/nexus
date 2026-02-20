package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"grain/nexus"

	"github.com/spf13/cobra"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var (
	submitDelegation  string
	submitPayload     string
	submitPayloadFile string
	submitMaxAttempts int
	submitPriority    int
	submitVersion     int
)

func newSubmitCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "submit",
		Short: "Submit an event to the queue",
		RunE:  runSubmit,
	}

	cmd.Flags().StringVar(&submitDelegation, "delegation", "", "Delegation type (plugin name)")
	cmd.Flags().StringVar(&submitPayload, "payload", "", "Event payload (JSON string)")
	cmd.Flags().StringVar(&submitPayloadFile, "payload-file", "", "Path to a JSON payload file")
	cmd.Flags().IntVar(&submitMaxAttempts, "max-attempts", 3, "Maximum attempts before dead-letter")
	cmd.Flags().IntVar(&submitPriority, "priority", 0, "Event priority (0-255)")
	cmd.Flags().IntVar(&submitVersion, "version", 1, "Event version")

	_ = cmd.MarkFlagRequired("delegation")
	return cmd
}

func runSubmit(cmd *cobra.Command, _ []string) error {
	payload, err := readPayload()
	if err != nil {
		return err
	}
	if submitDelegation == "" {
		return fmt.Errorf("delegation is required")
	}
	if submitPriority < 0 || submitPriority > 255 {
		return fmt.Errorf("priority must be between 0 and 255")
	}

	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()

	logger := newLogger()
	redisClient := newRedisClient()
	defer redisClient.Close()

	mongoClient, err := newMongoClient(ctx)
	if err != nil {
		return err
	}
	defer mongoClient.Disconnect(ctx)

	flow, err := newFlow(ctx, logger, redisClient, mongoClient)
	if err != nil {
		return err
	}
	defer flow.Close(500 * time.Millisecond)

	event := nexus.EventDetail{
		Id:             primitive.NewObjectID(),
		DelegationType: submitDelegation,
		Payload:        payload,
		MaxAttempts:    submitMaxAttempts,
		Priority:       uint8(submitPriority),
		Version:        submitVersion,
	}

	if err := flow.Push(ctx, event); err != nil {
		return err
	}

	printJSON("event_id", event.Id.Hex())
	return nil
}

func readPayload() (string, error) {
	if submitPayloadFile != "" {
		data, err := os.ReadFile(submitPayloadFile)
		if err != nil {
			return "", err
		}
		if len(data) == 0 {
			return "", fmt.Errorf("payload file is empty")
		}
		return string(data), nil
	}
	if submitPayload == "" {
		return "", fmt.Errorf("payload or payload-file is required")
	}
	return submitPayload, nil
}
