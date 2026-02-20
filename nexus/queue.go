// Package nexus provides task state queue implementations for managing task state persistence.
//
// # Overview
//
// This file provides two implementations of the TaskStateQueue interface:
//   - RedisTaskQueue: Production-ready Redis-backed queue for distributed systems
//   - InMemoryTaskQueue: Lightweight in-memory queue for local testing and development
//
// # Usage Examples
//
// Production setup with Redis:
//
//	// Create Redis cache
//	redisCache, err := cache.NewRedis(ctx, cache.RedisConfig{
//		Addr:   "localhost:6379",
//		Logger: logger,
//	})
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Create Redis task queue
//	taskQueue := nexus.NewRedisTaskQueue(redisCache, "task_state_queue")
//
//	// Use in NexusCore
//	core, err := nexus.NewNexusCore(ctx, nexus.NexusCoreBackendArgs{
//		TaskStateQueue: taskQueue,
//		// ... other config
//	})
//
// Local testing with in-memory queue:
//
//	// Create in-memory task queue
//	taskQueue := nexus.NewInMemoryTaskQueue()
//
//	// Use in NexusCore
//	core, err := nexus.NewNexusCore(ctx, nexus.NexusCoreBackendArgs{
//		TaskStateQueue: taskQueue,
//		// ... other config
//	})
package nexus

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/redis/go-redis/v9"
)

type RedisTaskQueue struct {
	client   *RedisCache
	queueKey string
}

func NewRedisTaskQueue(client *RedisCache, queueKey string) *RedisTaskQueue {
	return &RedisTaskQueue{client: client, queueKey: queueKey}
}

func (r *RedisTaskQueue) PushNewTaskState(state any) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return r.client.InternalCache.RPush(context.Background(), r.queueKey, data).Err()
}

func (r *RedisTaskQueue) PullNewTaskState(ctx context.Context) (any, error) {
	result, err := r.client.InternalCache.LPop(ctx, r.queueKey).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	if result == "" {
		return nil, nil
	}
	var state TaskState
	err = json.Unmarshal([]byte(result), &state)
	return state, err
}

func (r *RedisTaskQueue) IsEmpty(ctx context.Context) bool {
	length, err := r.client.InternalCache.LLen(ctx, r.queueKey).Result()
	return err == nil && length == 0
}

// InMemoryTaskQueue provides a simple in-memory implementation of TaskStateQueue.
// This implementation is thread-safe and suitable for local development and testing.
// For production deployments, use RedisTaskQueue instead.
type InMemoryTaskQueue struct {
	queue []TaskState
	mu    sync.RWMutex
}

func NewInMemoryTaskQueue() *InMemoryTaskQueue {
	return &InMemoryTaskQueue{
		queue: make([]TaskState, 0),
	}
}

func (m *InMemoryTaskQueue) PushNewTaskState(state any) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	taskState, ok := state.(TaskState)
	if !ok {
		return nil
	}

	m.queue = append(m.queue, taskState)
	return nil
}

func (m *InMemoryTaskQueue) PullNewTaskState(ctx context.Context) (any, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.queue) == 0 {
		return nil, nil
	}

	state := m.queue[0]
	m.queue = m.queue[1:]
	return state, nil
}

func (m *InMemoryTaskQueue) IsEmpty(ctx context.Context) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.queue) == 0
}
