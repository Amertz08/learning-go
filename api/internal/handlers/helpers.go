package handlers

import (
	"encoding/json"
	"net/http"
)

type ErrorResponse struct {
	Error string `json:"error"`
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
