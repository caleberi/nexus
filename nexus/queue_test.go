package nexus

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestInMemoryTaskQueue_PushPull(t *testing.T) {
	queue := NewInMemoryTaskQueue()
	ctx := context.Background()

	require.True(t, queue.IsEmpty(ctx))

	stateOne := TaskState{
		Id:        "task-1",
		EventId:   "event-1",
		Status:    "pending",
		UpdatedAt: time.Unix(100, 0).UTC(),
	}
	stateTwo := TaskState{
		Id:        "task-2",
		EventId:   "event-2",
		Status:    "running",
		UpdatedAt: time.Unix(200, 0).UTC(),
	}

	require.NoError(t, queue.PushNewTaskState(stateOne))
	require.NoError(t, queue.PushNewTaskState(stateTwo))
	require.False(t, queue.IsEmpty(ctx))

	pulled, err := queue.PullNewTaskState(ctx)
	require.NoError(t, err)
	require.Equal(t, stateOne, pulled)

	pulled, err = queue.PullNewTaskState(ctx)
	require.NoError(t, err)
	require.Equal(t, stateTwo, pulled)

	pulled, err = queue.PullNewTaskState(ctx)
	require.NoError(t, err)
	require.Nil(t, pulled)
	require.True(t, queue.IsEmpty(ctx))
}

func TestInMemoryTaskQueue_IgnoresNonTaskState(t *testing.T) {
	queue := NewInMemoryTaskQueue()
	ctx := context.Background()

	require.NoError(t, queue.PushNewTaskState("not-a-task"))
	require.True(t, queue.IsEmpty(ctx))
}

func TestRedisTaskQueue_PushPull(t *testing.T) {
	queue, cleanup := setupRedisTaskQueue(t)
	defer cleanup()

	ctx := context.Background()
	state := TaskState{
		Id:        "task-redis",
		EventId:   "event-redis",
		Status:    "completed",
		UpdatedAt: time.Unix(300, 0).UTC(),
	}

	require.NoError(t, queue.PushNewTaskState(state))
	require.False(t, queue.IsEmpty(ctx))

	pulled, err := queue.PullNewTaskState(ctx)
	require.NoError(t, err)
	require.Equal(t, state, pulled)
	require.True(t, queue.IsEmpty(ctx))
}

func TestRedisTaskQueue_Empty(t *testing.T) {
	queue, cleanup := setupRedisTaskQueue(t)
	defer cleanup()

	ctx := context.Background()

	pulled, err := queue.PullNewTaskState(ctx)
	require.NoError(t, err)
	require.Nil(t, pulled)
	require.True(t, queue.IsEmpty(ctx))
}

func setupRedisTaskQueue(t *testing.T) (*RedisTaskQueue, func()) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	redisCache := &RedisCache{InternalCache: client}
	queue := NewRedisTaskQueue(redisCache, "task_state_queue")

	cleanup := func() {
		_ = client.Close()
		mr.Close()
	}

	return queue, cleanup
}
