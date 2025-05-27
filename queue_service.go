package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type QueueService struct {
	redis *redis.Client
}

func NewQueueService(redis *redis.Client) *QueueService {
	return &QueueService{redis: redis}
}

func (qs *QueueService) GetWaitingPageInfo(ctx context.Context, eventID string) (*WaitingPageInfo, error) {
	// Get event sale start time from Redis or database
	saleStartKey := fmt.Sprintf("event:%s:sale_start", eventID)
	saleStartStr, err := qs.redis.Get(ctx, saleStartKey).Result()
	if err != nil {
		return nil, err
	}

	saleStartTime, err := time.Parse(time.RFC3339, saleStartStr)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	canEnterQueue := now.After(saleStartTime)
	countdownSecs := 0

	if !canEnterQueue {
		countdownSecs = int(saleStartTime.Sub(now).Seconds())
	}

	return &WaitingPageInfo{
		EventID:       eventID,
		SaleStartTime: saleStartTime,
		CountdownSecs: countdownSecs,
		CanEnterQueue: canEnterQueue,
	}, nil
}

func (qs *QueueService) EnterQueue(ctx context.Context, customerID, eventID string) (*QueueEntry, error) {
	queueKey := fmt.Sprintf("queue:%s", eventID)
	entryID := uuid.New().String()

	entry := &QueueEntry{
		ID:         entryID,
		CustomerID: customerID,
		EventID:    eventID,
		Status:     "waiting",
		CreatedAt:  time.Now(),
	}

	// Add to queue (Redis list)
	entryJSON, _ := json.Marshal(entry)
	position, err := qs.redis.LPush(ctx, queueKey, entryJSON).Result()
	if err != nil {
		return nil, err
	}

	entry.Position = int(position)

	// Store entry details
	entryKey := fmt.Sprintf("queue_entry:%s", entryID)
	qs.redis.Set(ctx, entryKey, entryJSON, 24*time.Hour)

	// Store customer's queue entry reference
	customerQueueKey := fmt.Sprintf("customer:%s:queue:%s", customerID, eventID)
	qs.redis.Set(ctx, customerQueueKey, entryID, 24*time.Hour)

	return entry, nil
}

func (qs *QueueService) GetQueueStatus(ctx context.Context, customerID, eventID string) (*QueueStatus, error) {
	customerQueueKey := fmt.Sprintf("customer:%s:queue:%s", customerID, eventID)
	entryID, err := qs.redis.Get(ctx, customerQueueKey).Result()
	if err != nil {
		return nil, err
	}

	entryKey := fmt.Sprintf("queue_entry:%s", entryID)
	entryJSON, err := qs.redis.Get(ctx, entryKey).Result()
	if err != nil {
		return nil, err
	}

	var entry QueueEntry
	json.Unmarshal([]byte(entryJSON), &entry)

	// Calculate current position
	queueKey := fmt.Sprintf("queue:%s", eventID)
	queueLength, _ := qs.redis.LLen(ctx, queueKey).Result()
	position := int(queueLength) - entry.Position + 1

	// Check if customer can proceed to processing
	canProceed := qs.canProceedToProcessing(ctx, customerID, eventID)

	return &QueueStatus{
		Position:          position,
		EstimatedWaitTime: position * 2, // 2 minutes per person estimate
		CanProceed:        canProceed,
	}, nil
}

func (qs *QueueService) canProceedToProcessing(ctx context.Context, customerID, eventID string) bool {
	processingKey := fmt.Sprintf("processing:%s:%s", eventID, customerID)
	exists, _ := qs.redis.Exists(ctx, processingKey).Result()
	return exists > 0
}
