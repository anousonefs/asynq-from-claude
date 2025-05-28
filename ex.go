package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/redis/go-redis/v9"
)

// processQueueForEvent processes only the next customer in queue if space is available
func (h *Handlers) processQueueForEvent2(ctx context.Context, eventID string) error {
	queueKey := fmt.Sprintf("queue:%s", eventID)
	processingSetKey := fmt.Sprintf("processing_set:%s", eventID)

	slog.Info("processQueueForEvent", "eventID", eventID)

	// Check if processing slots are available
	currentCount, err := h.ticketService.redis.SCard(ctx, processingSetKey).Result()
	if err != nil {
		return fmt.Errorf("failed to get processing count: %w", err)
	}

	if currentCount >= MAX_PROCESSING_CUSTOMERS {
		slog.Info("Processing queue full", "eventID", eventID, "currentCount", currentCount)
		return nil // No available slots, don't process anyone
	}

	// Only try to process the next customer in queue
	entryJSON, err := h.ticketService.redis.RPop(ctx, queueKey).Result()
	if err == redis.Nil {
		slog.Info("Queue is empty", "eventID", eventID)
		return nil // Queue is empty
	}
	if err != nil {
		return fmt.Errorf("failed to pop from queue: %w", err)
	}

	var entry QueueEntry
	if err := json.Unmarshal([]byte(entryJSON), &entry); err != nil {
		slog.Error("Failed to unmarshal queue entry", "error", err)
		return fmt.Errorf("failed to unmarshal queue entry: %w", err)
	}

	// Try to move to processing
	success, err := h.ticketService.TryEnterProcessing(ctx, entry.CustomerID, eventID)
	if err != nil {
		// Put the customer back at the front of the queue if there was an error
		entryJSONBytes, _ := json.Marshal(entry)
		h.ticketService.redis.LPush(ctx, queueKey, string(entryJSONBytes))
		return fmt.Errorf("failed to enter processing: %w", err)
	}

	if success {
		// Notify customer they can proceed
		h.scheduleNotification(entry.CustomerID, eventID, "You can now select your tickets!", "proceed")
		slog.Info("Customer moved to processing", "customerID", entry.CustomerID, "eventID", eventID)
	} else {
		// If couldn't enter processing, put them back at the front of queue
		entryJSONBytes, _ := json.Marshal(entry)
		h.ticketService.redis.LPush(ctx, queueKey, string(entryJSONBytes))
		slog.Info("Customer returned to queue front", "customerID", entry.CustomerID, "eventID", eventID)
	}

	return nil
}

// Enhanced event-driven approach: Call this when someone leaves processing
func (h *Handlers) onCustomerLeftProcessing(ctx context.Context, eventID string) {
	// When someone leaves processing, try to process the next person in queue
	go func() {
		if err := h.processQueueForEvent(ctx, eventID); err != nil {
			slog.Error("Failed to process queue after customer left", "eventID", eventID, "error", err)
		}
	}()
}

// Alternative: Use Redis pub/sub for even more efficient event-driven processing
func (h *Handlers) setupQueueProcessor(ctx context.Context) {
	// Subscribe to processing slot availability events
	pubsub := h.ticketService.redis.Subscribe(ctx, "processing_slot_available")
	defer pubsub.Close()

	ch := pubsub.Channel()

	for msg := range ch {
		eventID := msg.Payload
		slog.Info("Processing slot available", "eventID", eventID)

		// Try to process next customer for this event
		if err := h.processQueueForEvent(ctx, eventID); err != nil {
			slog.Error("Failed to process queue", "eventID", eventID, "error", err)
		}
	}
}

// Call this when a customer finishes or leaves processing
func (h *Handlers) notifyProcessingSlotAvailable(ctx context.Context, eventID string) {
	h.ticketService.redis.Publish(ctx, "processing_slot_available", eventID)
}
