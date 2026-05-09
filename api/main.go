package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.come/Amertz08/learning-go/api/internal/handlers"
)

func main() {
	ctx, cancel := initContext()
	defer cancel()

	httpServer := newServer("localhost", "8080")

	// start the web server
	go func() {
		fmt.Println("starting server")
		if err := httpServer.ListenAndServe(); err != nil {
			fmt.Println("error running server", err)
		}
	}()

	gracefulShutDown(ctx, httpServer)
}

// initContext creates a context and cancel function that listens for an interrupt signal
func initContext() (context.Context, context.CancelFunc) {
	ctx := context.Background()
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	return ctx, cancel
}

type IndexResponse struct {
	Params map[string][]string `json:"params"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

func newServer(host, port string) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		for k, v := range r.URL.Query() {
			fmt.Println(k, v)
		}
		resp := &IndexResponse{Params: r.URL.Query()}

		writeJSONResponse(w, http.StatusOK, resp)
	})
	mux.HandleFunc("POST /sum", handlers.SumHandler)
	httpServer := &http.Server{
		Addr:    net.JoinHostPort(host, port),
		Handler: mux,
	}
	return httpServer
}

func writeJSONResponse(w http.ResponseWriter, status int, data any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		writeServerErrorResponse(w, "server error")
		return
	}
}

func writeClientErrorResponse(w http.ResponseWriter, message string) {
	writeErrorResponse(w, http.StatusBadRequest, message)
}

func writeServerErrorResponse(w http.ResponseWriter, message string) {
	writeErrorResponse(w, http.StatusInternalServerError, message)
}

func writeErrorResponse(w http.ResponseWriter, status int, message string) {
	writeJSONResponse(w, status, &ErrorResponse{Error: message})
}

// gracefulShutDown handles graceful shutdown of the server
func gracefulShutDown(ctx context.Context, server *http.Server) {
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		// block until main context ends
		<-ctx.Done()

		// create a shutdown context with a timeout to begin graceful shutdown
		shutdownCtx := context.Background()
		shutdownCtx, cancel := context.WithTimeout(shutdownCtx, 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			fmt.Fprintf(os.Stderr, "error shutting down http server: %s\n", err)
		}
	}()

	wg.Wait()
}
