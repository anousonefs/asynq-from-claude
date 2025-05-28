package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
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
	// saleStartKey := fmt.Sprintf("event:%s:sale_start", eventID)
	// _, err := qs.redis.Get(ctx, saleStartKey).Result()
	// if err != nil {
	// 	return nil, fmt.Errorf("qs.redis.Get(saleStartKey: %v): %w", saleStartKey, err)
	// }

	startTime := "2025-05-28T13:30:05+07:00"

	saleStartTime, err := time.Parse(time.RFC3339, startTime)
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
	fmt.Printf("=> check can process key: %v, exist: %v\n", processingKey, exists)
	return exists > 0
}

// CleanQueue removes all entries from a specific queue and cleans up related data
func (qs *QueueService) CleanQueue(ctx context.Context, eventID string) error {
	queueKey := fmt.Sprintf("queue:%s", eventID)

	// Get all entries in the queue before deleting
	entries, err := qs.redis.LRange(ctx, queueKey, 0, -1).Result()
	if err != nil {
		return fmt.Errorf("failed to get queue entries: %w", err)
	}

	// Parse entries to get IDs for cleanup
	var entryIDs []string
	var customerQueueKeys []string

	for _, entryJSON := range entries {
		var entry QueueEntry
		if err := json.Unmarshal([]byte(entryJSON), &entry); err != nil {
			// Log but continue with cleanup
			slog.Error("Failed to unmarshal queue entry during cleanup", "error", err)
			continue
		}

		// Collect keys to delete
		entryKey := fmt.Sprintf("queue_entry:%s", entry.ID)
		customerQueueKey := fmt.Sprintf("customer:%s:queue:%s", entry.CustomerID, eventID)

		entryIDs = append(entryIDs, entryKey)
		customerQueueKeys = append(customerQueueKeys, customerQueueKey)
	}

	// Use pipeline for efficient deletion
	pipe := qs.redis.TxPipeline()

	// Delete the main queue
	pipe.Del(ctx, queueKey)

	// Delete all entry details
	if len(entryIDs) > 0 {
		pipe.Del(ctx, entryIDs...)
	}

	// Delete all customer queue references
	if len(customerQueueKeys) > 0 {
		pipe.Del(ctx, customerQueueKeys...)
	}

	// Execute all deletions
	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to clean queue: %w", err)
	}

	slog.Info("Queue cleaned successfully", "eventID", eventID, "entriesRemoved", len(entries))
	return nil
}

// CleanQueueByKey removes all entries from a queue using the direct queue key
func (qs *QueueService) CleanQueueByKey(ctx context.Context, queueKey string) error {
	// Extract eventID from queueKey (assuming format "queue:{eventID}")
	parts := strings.Split(queueKey, ":")
	if len(parts) != 2 || parts[0] != "queue" {
		return fmt.Errorf("invalid queue key format: %s", queueKey)
	}
	eventID := parts[1]

	// Use the main CleanQueue function
	return qs.CleanQueue(ctx, eventID)
}

// CleanExpiredQueueEntries removes expired entries from a queue
func (qs *QueueService) CleanExpiredQueueEntries(ctx context.Context, eventID string, maxAge time.Duration) error {
	queueKey := fmt.Sprintf("queue:%s", eventID)

	// Get all entries in the queue
	entries, err := qs.redis.LRange(ctx, queueKey, 0, -1).Result()
	if err != nil {
		return fmt.Errorf("failed to get queue entries: %w", err)
	}

	cutoffTime := time.Now().Add(-maxAge)
	var expiredEntries []QueueEntry
	var validEntries []any

	// Separate expired and valid entries
	for _, entryJSON := range entries {
		var entry QueueEntry
		if err := json.Unmarshal([]byte(entryJSON), &entry); err != nil {
			slog.Error("Failed to unmarshal queue entry", "error", err)
			continue
		}

		if entry.CreatedAt.Before(cutoffTime) {
			expiredEntries = append(expiredEntries, entry)
		} else {
			validEntries = append(validEntries, entryJSON)
		}
	}

	if len(expiredEntries) == 0 {
		return nil // Nothing to clean
	}

	// Use pipeline for cleanup
	pipe := qs.redis.TxPipeline()

	// Clear the queue and rebuild with valid entries
	pipe.Del(ctx, queueKey)
	if len(validEntries) > 0 {
		pipe.RPush(ctx, queueKey, validEntries...)
	}

	// Clean up expired entry details and customer references
	for _, entry := range expiredEntries {
		entryKey := fmt.Sprintf("queue_entry:%s", entry.ID)
		customerQueueKey := fmt.Sprintf("customer:%s:queue:%s", entry.CustomerID, eventID)
		pipe.Del(ctx, entryKey, customerQueueKey)
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to clean expired entries: %w", err)
	}

	slog.Info("Expired queue entries cleaned", "eventID", eventID, "expiredCount", len(expiredEntries))
	return nil
}

// RemoveCustomerFromQueue removes a specific customer from the queue
func (qs *QueueService) RemoveCustomerFromQueue(ctx context.Context, customerID, eventID string) error {
	queueKey := fmt.Sprintf("queue:%s", eventID)
	customerQueueKey := fmt.Sprintf("customer:%s:queue:%s", customerID, eventID)

	// Get the customer's entry ID
	entryID, err := qs.redis.Get(ctx, customerQueueKey).Result()
	if err == redis.Nil {
		return nil // Customer not in queue
	}
	if err != nil {
		return fmt.Errorf("failed to get customer queue entry: %w", err)
	}

	// Get the entry details to find the JSON in the queue
	entryKey := fmt.Sprintf("queue_entry:%s", entryID)
	entryJSON, err := qs.redis.Get(ctx, entryKey).Result()
	if err == redis.Nil {
		// Entry doesn't exist, just clean up the customer reference
		qs.redis.Del(ctx, customerQueueKey)
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to get entry details: %w", err)
	}

	// Remove from queue list, entry details, and customer reference
	pipe := qs.redis.TxPipeline()
	pipe.LRem(ctx, queueKey, 0, entryJSON) // Remove all occurrences
	pipe.Del(ctx, entryKey)
	pipe.Del(ctx, customerQueueKey)

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to remove customer from queue: %w", err)
	}

	slog.Info("Customer removed from queue", "customerID", customerID, "eventID", eventID)
	return nil
}

// GetQueueStats returns statistics about the queue
func (qs *QueueService) GetQueueStats(ctx context.Context, eventID string) (*QueueStats, error) {
	queueKey := fmt.Sprintf("queue:%s", eventID)

	// Get queue length
	length, err := qs.redis.LLen(ctx, queueKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get queue length: %w", err)
	}

	if length == 0 {
		return &QueueStats{
			EventID:     eventID,
			Length:      0,
			OldestEntry: time.Time{},
			NewestEntry: time.Time{},
		}, nil
	}

	// Get first and last entries to determine age range
	pipe := qs.redis.Pipeline()
	firstCmd := pipe.LIndex(ctx, queueKey, 0)
	lastCmd := pipe.LIndex(ctx, queueKey, -1)
	_, err = pipe.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get queue boundaries: %w", err)
	}

	var oldestTime, newestTime time.Time

	// Parse first entry (oldest)
	if firstJSON := firstCmd.Val(); firstJSON != "" {
		var firstEntry QueueEntry
		if err := json.Unmarshal([]byte(firstJSON), &firstEntry); err == nil {
			oldestTime = firstEntry.CreatedAt
		}
	}

	// Parse last entry (newest)
	if lastJSON := lastCmd.Val(); lastJSON != "" {
		var lastEntry QueueEntry
		if err := json.Unmarshal([]byte(lastJSON), &lastEntry); err == nil {
			newestTime = lastEntry.CreatedAt
		}
	}

	return &QueueStats{
		EventID:     eventID,
		Length:      int(length),
		OldestEntry: oldestTime,
		NewestEntry: newestTime,
	}, nil
}

// QueueStats represents queue statistics
type QueueStats struct {
	EventID     string    `json:"event_id"`
	Length      int       `json:"length"`
	OldestEntry time.Time `json:"oldest_entry"`
	NewestEntry time.Time `json:"newest_entry"`
}
