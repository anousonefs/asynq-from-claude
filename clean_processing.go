package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// CleanProcessingQueue removes all customers from processing set and cleans up related data
func (ts *TicketService) CleanProcessingQueue(ctx context.Context, eventID string) error {
	processingSetKey := fmt.Sprintf("processing_set:%s", eventID)

	// Get all customers in the processing set
	customerIDs, err := ts.redis.SMembers(ctx, processingSetKey).Result()
	if err != nil {
		return fmt.Errorf("failed to get processing set members: %w", err)
	}

	if len(customerIDs) == 0 {
		return nil // Nothing to clean
	}

	// Collect all processing keys to delete
	var processingKeys []string
	for _, customerID := range customerIDs {
		processingKey := fmt.Sprintf("processing:%s:%s", eventID, customerID)
		processingKeys = append(processingKeys, processingKey)
	}

	// Use pipeline for efficient deletion
	pipe := ts.redis.TxPipeline()

	// Delete the processing set
	pipe.Del(ctx, processingSetKey)

	// Delete all processing slot details
	if len(processingKeys) > 0 {
		pipe.Del(ctx, processingKeys...)
	}

	// Execute all deletions
	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to clean processing queue: %w", err)
	}

	slog.Info("Processing queue cleaned successfully", "eventID", eventID, "customersRemoved", len(customerIDs))
	return nil
}

// CleanProcessingQueueByKey removes all customers from processing using the direct set key
func (ts *TicketService) CleanProcessingQueueByKey(ctx context.Context, processingSetKey string) error {
	// Extract eventID from processingSetKey (assuming format "processing_set:{eventID}")
	parts := strings.Split(processingSetKey, ":")
	if len(parts) != 2 || parts[0] != "processing_set" {
		return fmt.Errorf("invalid processing set key format: %s", processingSetKey)
	}
	eventID := parts[1]

	// Use the main CleanProcessingQueue function
	return ts.CleanProcessingQueue(ctx, eventID)
}

// RemoveCustomerFromProcessing removes a specific customer from processing
func (ts *TicketService) RemoveCustomerFromProcessing(ctx context.Context, customerID, eventID string) error {
	processingSetKey := fmt.Sprintf("processing_set:%s", eventID)
	processingKey := fmt.Sprintf("processing:%s:%s", eventID, customerID)

	// Use pipeline to remove from both set and individual processing record
	pipe := ts.redis.TxPipeline()
	pipe.SRem(ctx, processingSetKey, customerID)
	pipe.Del(ctx, processingKey)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to remove customer from processing: %w", err)
	}

	fmt.Printf("=> removed customerId: %v, eventId: %v from processing\n", customerID, eventID)
	return nil
}

// CleanExpiredProcessingSlots removes expired processing slots
func (ts *TicketService) CleanExpiredProcessingSlots(ctx context.Context, eventID string) error {
	processingSetKey := fmt.Sprintf("processing_set:%s", eventID)

	// Get all customers in processing
	customerIDs, err := ts.redis.SMembers(ctx, processingSetKey).Result()
	if err != nil {
		return fmt.Errorf("failed to get processing set members: %w", err)
	}

	if len(customerIDs) == 0 {
		return nil
	}

	now := time.Now()
	var expiredCustomers []string
	var expiredKeys []string

	// Check each processing slot for expiration
	for _, customerID := range customerIDs {
		processingKey := fmt.Sprintf("processing:%s:%s", eventID, customerID)

		slotData, err := ts.redis.Get(ctx, processingKey).Result()
		if err == redis.Nil {
			// Processing key doesn't exist, remove from set
			expiredCustomers = append(expiredCustomers, customerID)
			continue
		}
		if err != nil {
			slog.Error("Failed to get processing slot", "customerID", customerID, "error", err)
			continue
		}

		var slot ProcessingSlot
		if err := json.Unmarshal([]byte(slotData), &slot); err != nil {
			slog.Error("Failed to unmarshal processing slot", "customerID", customerID, "error", err)
			// Remove corrupted data
			expiredCustomers = append(expiredCustomers, customerID)
			expiredKeys = append(expiredKeys, processingKey)
			continue
		}

		// Check if expired
		if now.After(slot.ExpiresAt) {
			expiredCustomers = append(expiredCustomers, customerID)
			expiredKeys = append(expiredKeys, processingKey)
		}
	}

	if len(expiredCustomers) == 0 {
		return nil // Nothing expired
	}

	// Remove expired entries
	pipe := ts.redis.TxPipeline()

	// Remove from processing set
	for _, customerID := range expiredCustomers {
		pipe.SRem(ctx, processingSetKey, customerID)
	}

	// Remove processing keys
	if len(expiredKeys) > 0 {
		pipe.Del(ctx, expiredKeys...)
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to clean expired processing slots: %w", err)
	}

	slog.Info("Expired processing slots cleaned", "eventID", eventID, "expiredCount", len(expiredCustomers))
	return nil
}

// GetProcessingStats returns statistics about the processing queue
func (ts *TicketService) GetProcessingStats(ctx context.Context, eventID string) (*ProcessingStats, error) {
	processingSetKey := fmt.Sprintf("processing_set:%s", eventID)

	// Get processing count
	count, err := ts.redis.SCard(ctx, processingSetKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get processing count: %w", err)
	}

	// Get all customer IDs
	customerIDs, err := ts.redis.SMembers(ctx, processingSetKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get processing members: %w", err)
	}

	var oldestStart, newestStart time.Time
	var activeSlots int

	// Analyze processing slots
	for i, customerID := range customerIDs {
		processingKey := fmt.Sprintf("processing:%s:%s", eventID, customerID)

		slotData, err := ts.redis.Get(ctx, processingKey).Result()
		if err != nil {
			continue
		}

		var slot ProcessingSlot
		if err := json.Unmarshal([]byte(slotData), &slot); err != nil {
			continue
		}

		activeSlots++

		if i == 0 {
			oldestStart = slot.StartedAt
			newestStart = slot.StartedAt
		} else {
			if slot.StartedAt.Before(oldestStart) {
				oldestStart = slot.StartedAt
			}
			if slot.StartedAt.After(newestStart) {
				newestStart = slot.StartedAt
			}
		}
	}

	return &ProcessingStats{
		EventID:        eventID,
		TotalSlots:     int(count),
		ActiveSlots:    activeSlots,
		MaxSlots:       MAX_PROCESSING_CUSTOMERS,
		OldestStart:    oldestStart,
		NewestStart:    newestStart,
		AvailableSlots: MAX_PROCESSING_CUSTOMERS - int(count),
	}, nil
}

// ProcessingStats represents processing queue statistics
type ProcessingStats struct {
	EventID        string    `json:"event_id"`
	TotalSlots     int       `json:"total_slots"`
	ActiveSlots    int       `json:"active_slots"`
	MaxSlots       int       `json:"max_slots"`
	AvailableSlots int       `json:"available_slots"`
	OldestStart    time.Time `json:"oldest_start"`
	NewestStart    time.Time `json:"newest_start"`
}

// ForceRemoveFromProcessing forcefully removes a customer and triggers queue processing
func (ts *TicketService) ForceRemoveFromProcessing(ctx context.Context, customerID, eventID string) error {
	// Remove from processing
	if err := ts.RemoveCustomerFromProcessing(ctx, customerID, eventID); err != nil {
		return err
	}

	// Trigger queue processing for this event (if you have the queue processor)
	// This would typically call your processQueueForEvent function
	// go ts.processQueueForEvent(ctx, eventID)

	return nil
}
