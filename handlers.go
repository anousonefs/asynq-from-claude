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

type Handlers struct {
	queueService        *QueueService
	ticketService       *TicketService
	notificationService *NotificationService
	asynqClient         *asynq.Client
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
	err := h.ticketService.Booking(ctx, req.CustomerID, req.EventID)
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
