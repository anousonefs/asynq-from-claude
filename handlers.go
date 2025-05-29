package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/hibiken/asynq"
	"github.com/labstack/echo/v4"
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

type Handlers struct {
	queueService  *QueueService
	ticketService *TicketService
	asynqClient   *asynq.Client
	pubNub        *PubNubService
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

	notification := NotificationMessage{
		ID:     fmt.Sprintf("notif_%d", time.Now().UnixNano()),
		Type:   payload.Type,
		Title:  "noti",
		Text:   payload.Message,
		Sender: "event-team",
		Data:   payload,
	}

	if err := h.pubNub.SendToCustomer(payload.CustomerID, notification); err != nil {
		return err
	}

	return nil
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

func (h *Handlers) GetWaitingPage(c echo.Context) error {
	eventID := c.Param("eventId")

	info, err := h.queueService.GetWaitingPageInfo(c.Request().Context(), eventID)
	if err != nil {
		slog.Error(fmt.Sprintf("h.queueService.GetWaitingPageInfo(%v)", eventID), "error", err)
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, info)
}

func (h *Handlers) EnterQueue(c echo.Context) error {
	var req struct {
		CustomerID string `json:"customer_id"`
		EventID    string `json:"event_id"`
	}

	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request"})
	}

	entry, err := h.queueService.EnterQueue(c.Request().Context(), req.CustomerID, req.EventID)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	// Schedule queue processing
	h.scheduleQueueProcessor(req.EventID)

	return c.JSON(http.StatusOK, entry)
}

func (h *Handlers) GetQueueStatus(c echo.Context) error {
	customerID := c.QueryParam("customer_id")
	eventID := c.Param("eventId")

	status, err := h.queueService.GetQueueStatus(c.Request().Context(), customerID, eventID)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, status)
}

func (h *Handlers) LockSeat(c echo.Context) error {
	var req struct {
		CustomerID string `json:"customer_id"`
		EventID    string `json:"event_id"`
		SeatID     string `json:"seat_id"`
	}

	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request"})
	}

	err := h.ticketService.LockSeat(c.Request().Context(), req.CustomerID, req.EventID, req.SeatID)
	if err != nil {
		return c.JSON(http.StatusConflict, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{"status": "seat locked"})
}

// Helper methods for scheduling tasks
func (h *Handlers) scheduleQueueProcessor(eventID string) {
	payload := QueueProcessorPayload{EventID: eventID}
	payloadByte, _ := json.Marshal(payload)
	task := asynq.NewTask(TypeQueueProcessor, payloadByte)
	h.asynqClient.Enqueue(task, asynq.ProcessIn(1*time.Second))
}

func (h *Handlers) scheduleNotification(customerID, eventID, message, msgType string) {
	payload := NotifyCustomerPayload{
		CustomerID: customerID,
		EventID:    eventID,
		Message:    message,
		Type:       msgType,
	}
	payloadByte, _ := json.Marshal(payload)
	task := asynq.NewTask(TypeNotifyCustomer, payloadByte)
	h.asynqClient.Enqueue(task)
}

type Book struct {
	EventID    string `json:"event_id"`
	TicketID   string `json:"ticket_id"`
	CustomerID string `json:"customer_id"`
}

func (h *Handlers) Book(c echo.Context) error {
	var req Book
	eventID := c.Param("eventId")

	if err := c.Bind(&req); err != nil {
		slog.Error("c.Bind()", "error", err)
		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}

	if eventID == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "eventID is empty"})
	}

	req.EventID = eventID

	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	err := h.ticketService.Book(ctx, req.CustomerID, req.EventID)
	if err != nil {
		slog.Error(fmt.Sprintf("h.ticketService.Booking(custId: %v, eventID: %v)", req.CustomerID, req.EventID), "error", err)
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, "success")
}

func (h *Handlers) CleanQueue(c echo.Context) error {
	eventID := c.Param("eventId")

	if eventID == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "eventID is empty"})
	}

	ctx := c.Request().Context()
	err := h.queueService.CleanQueue(ctx, eventID)
	if err != nil {
		slog.Error(fmt.Sprintf("h.queueService.CleanQueue(eventID: %v)", eventID), "error", err)
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, "success")
}

func (h *Handlers) CleanProcessingQueue(c echo.Context) error {
	eventID := c.Param("eventId")

	if eventID == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "eventID is empty"})
	}

	ctx := c.Request().Context()
	err := h.ticketService.CleanProcessingQueue(ctx, eventID)
	if err != nil {
		slog.Error(fmt.Sprintf("h.ticketService.CleanProcessingQueue(eventID: %v)", eventID), "error", err)
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, "success")
}

// Update queue status
func (h *Handlers) UpdateQueueStatus(c echo.Context) error {
	var req QueueStatusMessage

	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "eventID is empty"})
	}

	if err := h.pubNub.SendQueueUpdate(req.EventID, req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "eventID is empty"})
	}

	return c.JSON(http.StatusOK, "success")
}

// Update ticket status
func (h *Handlers) UpdateTicketStatus(c echo.Context) error {
	var req TicketUpdateMessage

	if err := c.Bind(&req); err != nil {
		return c.JSON(400, "bind error")
	}

	if err := h.pubNub.SendTicketUpdate(req.EventID, req); err != nil {
		return c.JSON(500, "error")
	}

	return c.JSON(200, "success")
}

// Broadcast message
func (h *Handlers) Broadcast(c echo.Context) error {
	var req struct {
		Channel string `json:"channel" binding:"required"`
		Message any    `json:"message" binding:"required"`
	}

	if err := c.Bind(&req); err != nil {
		return c.JSON(400, "bind error")
	}

	if err := h.pubNub.Broadcast(req.Channel, req.Message); err != nil {
		return c.JSON(500, "error")
	}

	return c.JSON(200, "success")
}

func (h *Handlers) SendNotification(c echo.Context) error {
	var req struct {
		UserID  string `json:"user_id" binding:"required"`
		Type    string `json:"type" binding:"required"`
		Title   string `json:"title" binding:"required"`
		Message string `json:"message" binding:"required"`
		Data    any    `json:"data,omitempty"`
	}

	if err := c.Bind(&req); err != nil {
		return c.JSON(400, "bind erro")
	}

	notification := NotificationMessage{
		ID:     fmt.Sprintf("notif_%d", time.Now().UnixNano()),
		Type:   req.Type,
		Title:  req.Title,
		Text:   req.Message,
		Sender: req.UserID,
		Data:   req.Data,
	}

	if err := h.pubNub.SendToCustomer(req.UserID, notification); err != nil {
		return c.JSON(500, "error")
	}

	return c.JSON(200, "success")
}
