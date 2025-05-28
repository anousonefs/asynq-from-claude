package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
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

// func (ts *TicketService) LockSeat(ctx context.Context, customerID, eventID, seatID string) error {
// 	seatLockKey := fmt.Sprintf("seat_lock:%s:%s", eventID, seatID)
//
// 	exists, err := ts.redis.Exists(ctx, seatLockKey).Result()
// 	if err != nil {
// 		return err
// 	}
// 	if exists > 0 {
// 		return fmt.Errorf("seat already locked")
// 	}
//
// 	// Lock the seat
// 	lock := &SeatLock{
// 		SeatID:     seatID,
// 		CustomerID: customerID,
// 		EventID:    eventID,
// 		ExpiresAt:  time.Now().Add(SEAT_LOCK_TIMEOUT),
// 	}
//
// 	lockJSON, _ := json.Marshal(lock)
// 	return ts.redis.Set(ctx, seatLockKey, lockJSON, SEAT_LOCK_TIMEOUT).Err()
// }

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

func (ts *TicketService) LockSeat(ctx context.Context, customerID, eventID, seatID string) error {
	seatLockKey := fmt.Sprintf("seat_lock:%s:%s", eventID, seatID)

	// Check if seat is already locked
	exists, err := ts.redis.Exists(ctx, seatLockKey).Result()
	if err != nil {
		return err
	}
	if exists > 0 {
		return fmt.Errorf("seat already locked")
	}

	// Unlock any previous seat held by this customer
	if err := ts.unlockPreviousSeat(ctx, customerID, eventID); err != nil {
		// Log error but don't fail the operation
		slog.Error("Failed to unlock previous seat", "customerID", customerID, "eventID", eventID, "error", err)
	}

	// Lock the new seat
	lock := &SeatLock{
		SeatID:     seatID,
		CustomerID: customerID,
		EventID:    eventID,
		ExpiresAt:  time.Now().Add(SEAT_LOCK_TIMEOUT),
	}
	lockJSON, _ := json.Marshal(lock)

	// Use a transaction to ensure atomicity
	pipe := ts.redis.TxPipeline()

	// Set the seat lock
	pipe.Set(ctx, seatLockKey, lockJSON, SEAT_LOCK_TIMEOUT)

	// Track which seat this customer has locked
	customerSeatKey := fmt.Sprintf("customer_seat:%s:%s", eventID, customerID)
	pipe.Set(ctx, customerSeatKey, seatID, SEAT_LOCK_TIMEOUT)

	_, err = pipe.Exec(ctx)
	return err
}

// Method 1: Track customer's current seat
func (ts *TicketService) unlockPreviousSeat(ctx context.Context, customerID, eventID string) error {
	customerSeatKey := fmt.Sprintf("customer_seat:%s:%s", eventID, customerID)

	// Get the seat ID that this customer currently has locked
	previousSeatID, err := ts.redis.Get(ctx, customerSeatKey).Result()
	if err == redis.Nil {
		// Customer doesn't have any seat locked
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to get customer's previous seat: %w", err)
	}

	// Unlock the previous seat
	previousSeatLockKey := fmt.Sprintf("seat_lock:%s:%s", eventID, previousSeatID)

	// Verify it's actually locked by this customer before unlocking
	seatLockData, err := ts.redis.Get(ctx, previousSeatLockKey).Result()
	if err == redis.Nil {
		// Seat lock already expired or doesn't exist
		ts.redis.Del(ctx, customerSeatKey) // Clean up tracking
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to get seat lock data: %w", err)
	}

	var seatLock SeatLock
	if err := json.Unmarshal([]byte(seatLockData), &seatLock); err != nil {
		return fmt.Errorf("failed to unmarshal seat lock: %w", err)
	}

	// Only unlock if it's actually locked by this customer
	if seatLock.CustomerID == customerID {
		pipe := ts.redis.TxPipeline()
		pipe.Del(ctx, previousSeatLockKey)
		pipe.Del(ctx, customerSeatKey)
		_, err = pipe.Exec(ctx)
		return err
	}

	return nil
}

// Method 2: Alternative approach using Redis patterns (less efficient but no extra tracking)
func (ts *TicketService) unlockPreviousSeatByPattern(ctx context.Context, customerID, eventID string) error {
	// Search for all seat locks for this event
	pattern := fmt.Sprintf("seat_lock:%s:*", eventID)
	keys, err := ts.redis.Keys(ctx, pattern).Result()
	if err != nil {
		return fmt.Errorf("failed to get seat lock keys: %w", err)
	}

	// Check each seat lock to find one owned by this customer
	for _, key := range keys {
		seatLockData, err := ts.redis.Get(ctx, key).Result()
		if err == redis.Nil {
			continue // Lock expired
		}
		if err != nil {
			continue // Skip on error
		}

		var seatLock SeatLock
		if err := json.Unmarshal([]byte(seatLockData), &seatLock); err != nil {
			continue // Skip malformed data
		}

		// If this seat is locked by our customer, unlock it
		if seatLock.CustomerID == customerID {
			ts.redis.Del(ctx, key)
			break // Customer should only have one seat locked
		}
	}

	return nil
}

// Helper method to unlock a specific seat (useful for cleanup)
func (ts *TicketService) UnlockSeat(ctx context.Context, customerID, eventID, seatID string) error {
	seatLockKey := fmt.Sprintf("seat_lock:%s:%s", eventID, seatID)
	customerSeatKey := fmt.Sprintf("customer_seat:%s:%s", eventID, customerID)

	// Verify the seat is locked by this customer
	seatLockData, err := ts.redis.Get(ctx, seatLockKey).Result()
	if err == redis.Nil {
		return nil // Already unlocked
	}
	if err != nil {
		return fmt.Errorf("failed to get seat lock: %w", err)
	}

	var seatLock SeatLock
	if err := json.Unmarshal([]byte(seatLockData), &seatLock); err != nil {
		return fmt.Errorf("failed to unmarshal seat lock: %w", err)
	}

	if seatLock.CustomerID != customerID {
		return fmt.Errorf("seat not locked by this customer")
	}

	// Remove both the seat lock and customer tracking
	pipe := ts.redis.TxPipeline()
	pipe.Del(ctx, seatLockKey)
	pipe.Del(ctx, customerSeatKey)
	_, err = pipe.Exec(ctx)
	return err
}

// Cleanup expired locks (run periodically)
func (ts *TicketService) CleanupExpiredSeatLocks(ctx context.Context, eventID string) error {
	pattern := fmt.Sprintf("seat_lock:%s:*", eventID)
	keys, err := ts.redis.Keys(ctx, pattern).Result()
	if err != nil {
		return err
	}

	now := time.Now()
	var expiredKeys []string

	for _, key := range keys {
		seatLockData, err := ts.redis.Get(ctx, key).Result()
		if err != nil {
			continue
		}

		var seatLock SeatLock
		if err := json.Unmarshal([]byte(seatLockData), &seatLock); err != nil {
			continue
		}

		if now.After(seatLock.ExpiresAt) {
			expiredKeys = append(expiredKeys, key)
			// Also clean up customer tracking
			customerSeatKey := fmt.Sprintf("customer_seat:%s:%s", eventID, seatLock.CustomerID)
			expiredKeys = append(expiredKeys, customerSeatKey)
		}
	}

	if len(expiredKeys) > 0 {
		return ts.redis.Del(ctx, expiredKeys...).Err()
	}

	return nil
}
