package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/hibiken/asynq"
)

const (
	TypeQueueProcessor = "queue:process"
	TypeTimeoutCleaner = "timeout:clean"
	TypeNotifyCustomer = "notify:customer"
)

// Task payloads
type QueueProcessorPayload struct {
	EventID string `json:"event_id"`
}

type TimeoutCleanerPayload struct {
	EventID string `json:"event_id"`
}

type NotifyCustomerPayload struct {
	CustomerID string `json:"customer_id"`
	EventID    string `json:"event_id"`
	Message    string `json:"message"`
	Type       string `json:"type"`
}

// Task handlers
func (h *Handlers) HandleQueueProcessor(ctx context.Context, t *asynq.Task) error {
	var payload QueueProcessorPayload
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		return err
	}

	// Process queue - move customers from waiting to processing
	return h.processQueueForEvent(ctx, payload.EventID)
}

func (h *Handlers) HandleTimeoutCleaner(ctx context.Context, t *asynq.Task) error {
	var payload TimeoutCleanerPayload
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		return err
	}

	// Clean expired processing slots and seat locks
	return h.cleanExpiredTimeouts(ctx, payload.EventID)
}

func (h *Handlers) HandleNotifyCustomer(ctx context.Context, t *asynq.Task) error {
	var payload NotifyCustomerPayload
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		return err
	}

	// Send notification via PubNub
	return h.notificationService.SendNotification(payload.CustomerID, payload.Message, payload.Type)
}

func (h *Handlers) processQueueForEvent(ctx context.Context, eventID string) error {
	queueKey := fmt.Sprintf("queue:%s", eventID)

	// Pop customers from queue and try to move them to processing
	for {
		slog.Info("processQueueForEvent", "info", queueKey)
		// Check how many processing slots are available
		processingSetKey := fmt.Sprintf("processing_set:%s", eventID)
		currentCount, _ := h.ticketService.redis.SCard(ctx, processingSetKey).Result()

		if currentCount >= MAX_PROCESSING_CUSTOMERS {
			// block
			break
		}

		// Get next customer from queue
		entryJSON, err := h.ticketService.redis.RPop(ctx, queueKey).Result()
		if err != nil {
			break // Queue is empty
		}

		var entry QueueEntry
		json.Unmarshal([]byte(entryJSON), &entry)

		fmt.Printf("try enter processing: customer_Id: %v, event_id: %v", entry.CustomerID, eventID)
		success, _ := h.ticketService.TryEnterProcessing(ctx, entry.CustomerID, eventID)
		if success {
			// Notify customer they can proceed
			h.scheduleNotification(entry.CustomerID, eventID, "You can now select your tickets!", "proceed")
		}
	}

	return nil
}

func (h *Handlers) cleanExpiredTimeouts(ctx context.Context, eventID string) error {
	// Clean expired processing slots
	processingSetKey := fmt.Sprintf("processing_set:%s", eventID)
	members, _ := h.ticketService.redis.SMembers(ctx, processingSetKey).Result()

	for _, customerID := range members {
		processingKey := fmt.Sprintf("processing:%s:%s", eventID, customerID)
		exists, _ := h.ticketService.redis.Exists(ctx, processingKey).Result()
		if exists == 0 {
			// Processing slot expired, remove from set
			h.ticketService.redis.SRem(ctx, processingSetKey, customerID)
		}
	}

	return nil
}
