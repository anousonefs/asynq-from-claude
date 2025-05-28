package main

import (
	"context"
	"log/slog"
)

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
