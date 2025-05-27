package main

import (
	"time"
)

type QueueEntry struct {
	ID         string     `json:"id"`
	CustomerID string     `json:"customer_id"`
	EventID    string     `json:"event_id"`
	Position   int        `json:"position"`
	Status     string     `json:"status"` // waiting, processing, completed, expired
	CreatedAt  time.Time  `json:"created_at"`
	ExpiresAt  *time.Time `json:"expires_at,omitempty"`
}

type ProcessingSlot struct {
	ID         string    `json:"id"`
	CustomerID string    `json:"customer_id"`
	EventID    string    `json:"event_id"`
	StartedAt  time.Time `json:"started_at"`
	ExpiresAt  time.Time `json:"expires_at"`
}

type SeatLock struct {
	SeatID     string    `json:"seat_id"`
	CustomerID string    `json:"customer_id"`
	EventID    string    `json:"event_id"`
	ExpiresAt  time.Time `json:"expires_at"`
}

type WaitingPageInfo struct {
	EventID       string    `json:"event_id"`
	SaleStartTime time.Time `json:"sale_start_time"`
	CountdownSecs int       `json:"countdown_seconds"`
	CanEnterQueue bool      `json:"can_enter_queue"`
}

type QueueStatus struct {
	Position          int  `json:"position"`
	EstimatedWaitTime int  `json:"estimated_wait_time_minutes"`
	CanProceed        bool `json:"can_proceed"`
}
