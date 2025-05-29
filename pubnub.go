package main

import (
	"fmt"
	"log"
	"time"

	pubnub "github.com/pubnub/go"
)

// Message structures
type NotificationMessage struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	Title     string    `json:"title"`
	Text      string    `json:"text"`
	Sender    string    `json:"sender,omitempty"`
	EventID   string    `json:"event_id,omitempty"`
	Timestamp time.Time `json:"timestamp"`
	Data      any       `json:"data,omitempty"`
}

type QueueStatusMessage struct {
	EventID      string `json:"event_id"`
	CustomerID   string `json:"customer_id"`
	Position     int    `json:"position"`
	Status       string `json:"status"`
	WaitTime     int    `json:"wait_time_minutes"`
	TotalInQueue int    `json:"total_in_queue"`
}

type TicketUpdateMessage struct {
	EventID    string `json:"event_id"`
	CustomerID string `json:"customer_id"`
	Action     string `json:"action"` // "seat_locked", "seat_unlocked", "ticket_purchased"
	SeatID     string `json:"seat_id,omitempty"`
	Message    string `json:"message"`
}

// PubNub Service
type PubNubService struct {
	client *pubnub.PubNub
}

func NewPubNubService(publishKey, subscribeKey, secretKey string) *PubNubService {
	config := pubnub.NewConfig() // Set a default UUID
	config.PublishKey = publishKey
	config.SubscribeKey = subscribeKey
	config.SecretKey = secretKey // Add if you have secret key

	pn := pubnub.NewPubNub(config)

	return &PubNubService{
		client: pn,
	}
}

// Send notification to specific user
func (p *PubNubService) SendToUser(userID string, message NotificationMessage) error {
	channel := fmt.Sprintf("user_%s", userID)
	message.Timestamp = time.Now()

	_, status, err := p.client.Publish().
		Channel(channel).
		Message(message).
		Execute()
	if err != nil {
		return fmt.Errorf("failed to publish to user channel: %w", err)
	}

	if status.Error != nil {
		return fmt.Errorf("pubnub error: %s", status.Error)
	}

	log.Printf("Message sent to user %s: %s", userID, message.Title)
	return nil
}

// Send queue status update
func (p *PubNubService) SendQueueUpdate(eventID string, message QueueStatusMessage) error {
	channel := fmt.Sprintf("queue_%s", eventID)

	_, status, err := p.client.Publish().
		Channel(channel).
		Message(message).
		Execute()
	if err != nil {
		return fmt.Errorf("failed to publish queue update: %w", err)
	}

	if status.Error != nil {
		return fmt.Errorf("pubnub error: %s", status.Error)
	}

	log.Printf("Queue update sent for event %s", eventID)
	return nil
}

// Send ticket update
func (p *PubNubService) SendTicketUpdate(eventID string, message TicketUpdateMessage) error {
	channel := fmt.Sprintf("tickets_%s", eventID)

	_, status, err := p.client.Publish().
		Channel(channel).
		Message(message).
		Execute()
	if err != nil {
		return fmt.Errorf("failed to publish ticket update: %w", err)
	}

	if status.Error != nil {
		return fmt.Errorf("pubnub error: %s", status.Error)
	}

	log.Printf("Ticket update sent for event %s: %s", eventID, message.Action)
	return nil
}

// Broadcast to all users
func (p *PubNubService) Broadcast(channel string, message any) error {
	_, status, err := p.client.Publish().
		Channel(channel).
		Message(message).
		Execute()
	if err != nil {
		return fmt.Errorf("failed to broadcast: %w", err)
	}

	if status.Error != nil {
		return fmt.Errorf("pubnub error: %s", status.Error)
	}

	log.Printf("Broadcast sent to channel %s", channel)
	return nil
}
