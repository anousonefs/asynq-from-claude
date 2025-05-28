package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	MAX_PROCESSING_CUSTOMERS = 10
	PROCESSING_TIMEOUT       = 10 * time.Minute
	SEAT_LOCK_TIMEOUT        = 5 * time.Minute
)

type TicketService struct {
	redis *redis.Client
}

func NewTicketService(redis *redis.Client) *TicketService {
	return &TicketService{redis: redis}
}

func (ts *TicketService) TryEnterProcessing(ctx context.Context, customerID, eventID string) (bool, error) {
	processingSetKey := fmt.Sprintf("processing_set:%s", eventID)

	// Check current processing count
	currentCount, err := ts.redis.SCard(ctx, processingSetKey).Result()
	if err != nil {
		return false, err
	}

	if currentCount >= MAX_PROCESSING_CUSTOMERS {
		return false, nil
	}

	// Try to add customer to processing set
	added, err := ts.redis.SAdd(ctx, processingSetKey, customerID).Result()
	if err != nil {
		return false, err
	}

	if added == 0 {
		return false, nil // Already in processing
	}

	// Create processing slot with timeout
	slot := &ProcessingSlot{
		ID:         uuid.New().String(),
		CustomerID: customerID,
		EventID:    eventID,
		StartedAt:  time.Now(),
		ExpiresAt:  time.Now().Add(PROCESSING_TIMEOUT),
	}

	slotJSON, _ := json.Marshal(slot)
	processingKey := fmt.Sprintf("processing:%s:%s", eventID, customerID)
	ts.redis.Set(ctx, processingKey, slotJSON, PROCESSING_TIMEOUT)

	return true, nil
}

func (ts *TicketService) LockSeat(ctx context.Context, customerID, eventID, seatID string) error {
	seatLockKey := fmt.Sprintf("seat_lock:%s:%s", eventID, seatID)

	exists, err := ts.redis.Exists(ctx, seatLockKey).Result()
	if err != nil {
		return err
	}
	if exists > 0 {
		return fmt.Errorf("seat already locked")
	}

	// Lock the seat
	lock := &SeatLock{
		SeatID:     seatID,
		CustomerID: customerID,
		EventID:    eventID,
		ExpiresAt:  time.Now().Add(SEAT_LOCK_TIMEOUT),
	}

	lockJSON, _ := json.Marshal(lock)
	return ts.redis.Set(ctx, seatLockKey, lockJSON, SEAT_LOCK_TIMEOUT).Err()
}

func (ts *TicketService) ReleaseSeat(ctx context.Context, eventID, seatID string) error {
	seatLockKey := fmt.Sprintf("seat_lock:%s:%s", eventID, seatID)
	return ts.redis.Del(ctx, seatLockKey).Err()
}

func (ts *TicketService) ReleaseProcessingSlot(ctx context.Context, customerID, eventID string) error {
	processingKey := fmt.Sprintf("processing:%s:%s", eventID, customerID)
	processingSetKey := fmt.Sprintf("processing_set:%s", eventID)

	// Remove from both processing record and set
	ts.redis.Del(ctx, processingKey)
	ts.redis.SRem(ctx, processingSetKey, customerID)

	return nil
}
