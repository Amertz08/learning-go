package handlers

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"
)

var (
	ErrEmptyRequest          = errors.New("empty request")
	ErrCouldNotDecodeRequest = errors.New("could not decode request")
)

func ShortenHandler(
	logger *slog.Logger,
	hasher HashService,
	store HashDataStore,
	cache HashCacheStore,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		data, err := decodeRequest[ShortenRequest](r.Body)

		if err != nil {
			if errors.As(err, &ErrEmptyRequest) {
				encodeClientErrorResponse(w, "invalid parameters")
				return
			}
			logger.Error("could not decode request", slog.Any("error", err))
			encodeServerErrorResponse(w, "server error")
			return
		}

		if !data.Valid() {
			encodeClientErrorResponse(w, "invalid parameters")
			return
		}

		encoded := hasher.Encode(data.URL)

		if err = store.Create(encoded, data.URL); err != nil {
			logger.Error("could not save shortened link", slog.Any("error", err))
			encodeServerErrorResponse(w, "server error")
			return
		}

		if err = cache.Set(data.URL, encoded, 30*time.Minute); err != nil {
			logger.Error("could not cache value", slog.Any("error", err))
			encodeServerErrorResponse(w, "server error")
			return
		}

		encodeResponse[ShortenedResponse](w, http.StatusOK, &ShortenedResponse{URL: encoded})
	}
}

type ShortenRequest struct {
	URL string `json:"url"`
}

func (r *ShortenRequest) Valid() bool {
	if r.URL == "" {
		return false
	}
	return true
}

type ShortenedResponse struct {
	URL string `json:"url"`
}

func VisitHandler(logger *slog.Logger, cache HashCacheStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hash := r.PathValue("short_hash")
		logger.Info("visiting", slog.Any("hash", hash))
		redirectUrl, ok := cache.Get(hash)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		http.Redirect(w, r, redirectUrl, http.StatusFound)
	}
}

func decodeRequest[T any](r io.ReadCloser) (*T, error) {
	if r == nil {
		return nil, ErrEmptyRequest
	}
	var data T
	if err := json.NewDecoder(r).Decode(&data); err != nil {
		return nil, ErrCouldNotDecodeRequest
	}
	return &data, nil
}

func encodeResponse[T any](w http.ResponseWriter, status int, data *T) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if err := json.NewEncoder(w).Encode(data); err != nil {
		encodeServerErrorResponse(w, "server error")
		return
	}
}

type ErrorResponse struct {
	Error string `json:"error"`
}

func encodeClientErrorResponse(w http.ResponseWriter, message string) {
	encodeResponse[ErrorResponse](w, http.StatusBadRequest, &ErrorResponse{Error: message})
}

func encodeServerErrorResponse(w http.ResponseWriter, message string) {
	encodeResponse[ErrorResponse](w, http.StatusInternalServerError, &ErrorResponse{Error: message})
}

type HashService interface {
	Encode(string) string
	Decode(string) string
}

type HashDataStore interface {
	Create(string, string) error
}

type HashCacheStore interface {
	Set(string, string, time.Duration) error
	Get(string) (string, bool)
}
