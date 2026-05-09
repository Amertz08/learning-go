package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type ErrorResponse struct {
	Error string `json:"error"`
}

func decodeRequest[T any](r *http.Request) (T, error) {
	var data T
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		return data, fmt.Errorf("error decoding: %w", err)
	}
	return data, nil
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
