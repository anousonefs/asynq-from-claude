// main.go - Entry point
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hibiken/asynq"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/redis/go-redis/v9"
)

func main() {
	redisOpt := asynq.RedisClientOpt{Addr: "localhost:6379"}
	redisClient := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	pubNubService := NewPubNubService(
		os.Getenv("PN_PUBLISH_KEY"),
		os.Getenv("PN_SUBSCRIBE_KEY"),
		os.Getenv("PN_SECRET_KEY"),
	)
	queueService := NewQueueService(redisClient)
	ticketService := NewTicketService(redisClient, pubNubService)
	asynqClient := asynq.NewClient(redisOpt)
	defer asynqClient.Close()

	handlers := &Handlers{
		queueService:  queueService,
		ticketService: ticketService,
		asynqClient:   asynqClient,
		pubNub:        pubNubService,
	}

	go startAsynqServer(redisOpt, handlers)

	e := echo.New()
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())
	e.Use(middleware.CORS())

	setupRoutes(e, handlers)

	go func() {
		if err := e.Start(":8081"); err != nil && err != http.ErrServerClosed {
			log.Fatal("Server failed to start:", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := e.Shutdown(ctx); err != nil {
		log.Fatal("Server forced to shutdown:", err)
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

		notification := NotificationMessage{
			ID:        fmt.Sprintf("test_%d", time.Now().UnixNano()),
			Type:      "proceed",
			Title:     "Test Notification",
			Text:      "This is a test notification from Go backend!",
			Sender:    userID,
			Timestamp: time.Now(),
		}

		if err := handlers.pubNub.SendToCustomer(userID, notification); err != nil {
			return c.JSON(http.StatusInternalServerError, "")
		}

		return c.JSON(http.StatusOK, "success")
	})
}

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
