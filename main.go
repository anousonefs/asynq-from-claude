// main.go - Entry point
package main

import (
	"context"
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
	// Initialize Redis
	redisOpt := asynq.RedisClientOpt{Addr: "localhost:6379"}
	redisClient := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	// Initialize services
	queueService := NewQueueService(redisClient)
	ticketService := NewTicketService(redisClient)
	notificationService := NewNotificationService() // PubNub integration

	// Initialize Asynq client and server
	asynqClient := asynq.NewClient(redisOpt)
	defer asynqClient.Close()

	// Initialize handlers
	handlers := &Handlers{
		queueService:        queueService,
		ticketService:       ticketService,
		notificationService: notificationService,
		asynqClient:         asynqClient,
	}

	// Start Asynq server in goroutine
	go startAsynqServer(redisOpt, handlers)

	// Setup Echo server
	e := echo.New()
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())
	e.Use(middleware.CORS())

	// Routes
	setupRoutes(e, handlers)

	// Start server
	go func() {
		if err := e.Start(":8080"); err != nil && err != http.ErrServerClosed {
			log.Fatal("Server failed to start:", err)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := e.Shutdown(ctx); err != nil {
		log.Fatal("Server forced to shutdown:", err)
	}
}

// types.go - Data structures

// queue_service.go - Queue management

// ticket_service.go - Ticket and seat management

// task_types.go - Asynq task definitions

// handlers.go - HTTP handlers

// notification_service.go - PubNub integration
