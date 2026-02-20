package server

import (
	"context"
	"fmt"
	"time"

	"grain/nexus"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/cenkalti/backoff/v4"
)

func submitEventToCore(
	core *nexus.NexusCore,
	delegationType string,
	payload string,
	maxAttempts int,
	priority uint8,
) (string, error) {
	if core == nil {
		return "", fmt.Errorf("nexus core is not initialized")
	}

	eventID := primitive.NewObjectID()

	event := nexus.EventDetail{
		Id:             eventID,
		DelegationType: delegationType,
		Payload:        payload,
		MaxAttempts:    maxAttempts,
		Priority:       priority,
		Version:        1,
	}

	submitBackoff := backoff.NewExponentialBackOff()
	submitBackoff.InitialInterval = 100 * time.Millisecond
	submitBackoff.MaxInterval = 1 * time.Second
	submitBackoff.MaxElapsedTime = 30 * time.Second

	if err := core.SubmitEvent(event, submitBackoff); err != nil {
		return "", fmt.Errorf("failed to submit event to nexus core: %w", err)
	}

	return eventID.Hex(), nil
}

func getEventList(
	core *nexus.NexusCore,
	page, limit int,
	state, delegationType string,
) ([]nexus.EventDetail, int64, error) {
	if core == nil {
		return nil, 0, fmt.Errorf("nexus core is not initialized")
	}

	if page < 1 {
		page = 1
	}
	if limit < 1 || limit > 100 {
		limit = 20
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	filter := bson.D{}
	if state != "" {
		filter = append(filter, bson.E{Key: "state", Value: state})
	}
	if delegationType != "" {
		filter = append(filter, bson.E{Key: "delegation_type", Value: delegationType})
	}

	repo := core.GetFlowEngine().GetRepo()

	count, err := repo.Count(ctx, filter)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count events: %w", err)
	}

	skip := int64((page - 1) * limit)
	findOptions := options.Find().
		SetSkip(skip).
		SetLimit(int64(limit)).
		SetSort(bson.D{{Key: "created_at", Value: -1}})

	events, err := repo.FindMany(ctx, filter, findOptions)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to fetch events: %w", err)
	}

	return events, count, nil
}

func getEventByID(core *nexus.NexusCore, eventID string) (*nexus.EventDetail, error) {
	if core == nil {
		return nil, fmt.Errorf("nexus core is not initialized")
	}

	objectID, err := primitive.ObjectIDFromHex(eventID)
	if err != nil {
		return nil, fmt.Errorf("invalid event ID: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	repo := core.GetFlowEngine().GetRepo()
	event, err := repo.FindOneById(ctx, objectID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch event: %w", err)
	}

	return event, nil
}
