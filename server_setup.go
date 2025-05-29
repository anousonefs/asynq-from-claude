package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/hibiken/asynq"
	"github.com/labstack/echo/v4"
)

func startAsynqServer(redisOpt asynq.RedisClientOpt, handlers *Handlers) {
	srv := asynq.NewServer(
		redisOpt,
		asynq.Config{
			Concurrency: 10,
			Queues: map[string]int{
				"critical": 6,
				"default":  3,
				"low":      1,
			},
		},
	)

	mux := asynq.NewServeMux()
	mux.HandleFunc(TypeQueueProcessor, handlers.HandleQueueProcessor)
	mux.HandleFunc(TypeTimeoutCleaner, handlers.HandleTimeoutCleaner)
	mux.HandleFunc(TypeNotifyCustomer, handlers.HandleNotifyCustomer)

	// Schedule periodic cleanup
	scheduler := asynq.NewScheduler(redisOpt, nil)

	timeoutByte, _ := json.Marshal(TimeoutCleanerPayload{EventID: "all"})

	scheduler.Register("*/1 * * * *", asynq.NewTask(TypeTimeoutCleaner, timeoutByte))

	go func() {
		if err := scheduler.Run(); err != nil {
			log.Fatal("Scheduler failed to start:", err)
		}
	}()

	if err := srv.Run(mux); err != nil {
		log.Fatal("Asynq server failed to start:", err)
	}
}

func setupRoutes(e *echo.Echo, handlers *Handlers) {
	api := e.Group("/api/v1")

	// Waiting page
	api.GET("/events/:eventId/waiting", handlers.GetWaitingPage)

	// Queue operations
	api.POST("/queue/enter", handlers.EnterQueue)
	api.GET("/events/:eventId/queue/status", handlers.GetQueueStatus)

	// Ticket operations
	api.POST("/seats/lock", handlers.LockSeat)

	api.POST("/events/:eventId/book", handlers.Book)
	api.POST("/events/:eventId/clean-queue", handlers.CleanQueue)
	api.POST("/events/:eventId/clean-processing-queue", handlers.CleanProcessingQueue)

	api.POST("/notify", handlers.SendNotification)
	api.POST("/queue/update", handlers.UpdateQueueStatus)
	api.POST("/tickets/update", handlers.UpdateTicketStatus)
	api.POST("/broadcast", handlers.Broadcast)

	// Example routes for testing
	api.GET("/test/notification/:userID", func(c echo.Context) error {
		userID := c.Param("userID")

		// const { message, publisher, timetoken } = messageEvent;
		// publisher

		notification := NotificationMessage{
			ID:        fmt.Sprintf("test_%d", time.Now().UnixNano()),
			Type:      "info",
			Title:     "Test Notification",
			Text:      "This is a test notification from Go backend!",
			Sender:    userID,
			Timestamp: time.Now(),
		}

		if err := handlers.pubNub.SendToUser(userID, notification); err != nil {
			return c.JSON(http.StatusInternalServerError, "")
		}

		return c.JSON(http.StatusOK, "success")
	})
}
