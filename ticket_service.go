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
	redis  *redis.Client
	pubnub *PubNubService
}

func NewTicketService(redis *redis.Client, pubnub *PubNubService) *TicketService {
	return &TicketService{redis: redis, pubnub: pubnub}
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
	fmt.Printf("=> add customerId: %v, eventId: %v to processing\n", customerID, eventID)

	return true, nil
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

func (ts *TicketService) Booking(ctx context.Context, customerID, eventID string) error {
	if err := ts.ReleaseProcessingSlot(ctx, customerID, eventID); err != nil {
		return fmt.Errorf("ts.ReleaseProcessingSlot(custID: %v, eventID: %v): %w", customerID, eventID, err)
	}

	// todo: close goroutine
	ts.onCustomerLeftProcessing(ctx, eventID)

	return nil
}

func (ts *TicketService) onCustomerLeftProcessing(ctx context.Context, eventID string) {
	// When someone leaves processing, try to process the next person in queue
	println("=> start")
	go func() {
		if err := ts.processQueueForEvent2(ctx, eventID); err != nil {
			slog.Error("ts.processQueueForEvent2", "eventID", eventID, "error", err)
		}
		println("=> end goroutine")
	}()
	println("=> ended")
}

func (ts *TicketService) processQueueForEvent2(ctx context.Context, eventID string) error {
	queueKey := fmt.Sprintf("queue:%s", eventID)
	processingSetKey := fmt.Sprintf("processing_set:%s", eventID)

	slog.Info("processQueueForEvent", "eventID", eventID)

	// Check if processing slots are available
	currentCount, err := ts.redis.SCard(ctx, processingSetKey).Result()
	if err != nil {
		return fmt.Errorf("failed to get processing count: %w", err)
	}

	if currentCount >= MAX_PROCESSING_CUSTOMERS {
		slog.Info("Processing queue full", "eventID", eventID, "currentCount", currentCount)
		return nil // No available slots, don't process anyone
	}

	// Only try to process the next customer in queue
	entryJSON, err := ts.redis.RPop(ctx, queueKey).Result()
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
	success, err := ts.TryEnterProcessing(ctx, entry.CustomerID, eventID)
	if err != nil {
		// Put the customer back at the front of the queue if there was an error
		entryJSONBytes, _ := json.Marshal(entry)
		ts.redis.LPush(ctx, queueKey, string(entryJSONBytes))
		return fmt.Errorf("failed to enter processing: %w", err)
	}

	if success {
		if err := ts.pubnub.SendToUser(entry.CustomerID, NotificationMessage{}); err != nil {
			slog.Error("ts.pubnub.Publish()", "error", err)
		}
		slog.Info("Customer moved to processing", "customerID", entry.CustomerID, "eventID", eventID)
	} else {
		// If couldn't enter processing, put them back at the front of queue
		entryJSONBytes, _ := json.Marshal(entry)
		ts.redis.LPush(ctx, queueKey, string(entryJSONBytes))
		slog.Info("Customer returned to queue front", "customerID", entry.CustomerID, "eventID", eventID)
	}

	return nil
}
