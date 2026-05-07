package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"
)

func main() {
	// create a context and listen for CTRL+C for cancellation
	ctx := context.Background()
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	// Build server
	server := http.NewServeMux()
	server.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("hello"))
	}))
	httpServer := &http.Server{
		Addr:    net.JoinHostPort("localhost", "8080"),
		Handler: server,
	}

	go func() {
		fmt.Println("starting server")
		if err := httpServer.ListenAndServe(); err != nil {
			fmt.Println("error running server", err)
		}
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// block until main context is ended
		<-ctx.Done()

		// create a shutdown context with a timeout to begin graceful shutdown
		shutdownCtx := context.Background()
		shutdownCtx, cancel := context.WithTimeout(shutdownCtx, 10*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			fmt.Fprintf(os.Stderr, "error shutting down http server: %s\n", err)
		}
	}()
	wg.Wait()
}
