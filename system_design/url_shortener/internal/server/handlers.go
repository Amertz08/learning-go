package server

import (
	"context"
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

// TODO: cache expiry via config

func ShortenHandler(
	logger *slog.Logger,
	hasher HashService,
	store HashDataStore,
	cache HashCacheStore,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		data, err := decodeRequest[ShortenRequest](r.Body)

		if err != nil {
			if errors.As(err, &ErrEmptyRequest) {
				encodeClientErrorResponse(w, "invalid parameters")
				return
			}
			logger.Error("could not decode request", slog.Any("error", err))
			encodeServerErrorResponse(w)
			return
		}

		if !data.Valid() {
			encodeClientErrorResponse(w, "invalid parameters")
			return
		}

		encoded := hasher.Encode(data.URL)

		shortRecord, err := store.CreateShortenedRecord(ctx, encoded, data.URL)
		if err != nil {
			logger.Error("could not save shortened link", slog.Any("error", err))
			encodeServerErrorResponse(w)
			return
		}

		if err = cache.Set(ctx, data.URL, shortRecord, 30*time.Minute); err != nil {
			logger.Error("could not cache value", slog.Any("error", err))
			encodeServerErrorResponse(w)
			return
		}

		// TODO: shorten prefix should be a config
		encodeResponse[ShortenedResponse](w, http.StatusOK, &ShortenedResponse{URL: "http://localhost:8080/v/" + encoded})
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

func VisitHandler(logger *slog.Logger, store HashDataStore, cache HashCacheStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		hash := r.PathValue("short_hash")

		shortRecord, err := cache.Get(ctx, hash)
		if err != nil {
			logger.Error("error getting hash from cache", slog.Any("error", err))
			encodeServerErrorResponse(w)
			return
		}
		if shortRecord == nil {
			shortRecord, err = store.Get(ctx, hash)
			if err != nil {
				logger.Error("error getting hash from DB", slog.Any("error", err))
				encodeServerErrorResponse(w)
				return
			}
			if shortRecord == nil {
				logger.Warn("not found", slog.Any("hash", hash))
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if err = cache.Set(ctx, hash, shortRecord, 30*time.Minute); err != nil {
				logger.Error("error setting the cache", slog.Any("error", err))
				encodeServerErrorResponse(w)
				return
			}
		}

		_, err = store.CreateVisitRecord(ctx, shortRecord.Id)
		if err != nil {
			logger.Error("error creating visit", slog.Any("error", err))
			encodeServerErrorResponse(w)
			return
		}

		http.Redirect(w, r, shortRecord.TargetURL, http.StatusFound)
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
		encodeServerErrorResponse(w)
		return
	}
}

type ErrorResponse struct {
	Error string `json:"error"`
}

func encodeClientErrorResponse(w http.ResponseWriter, message string) {
	encodeResponse[ErrorResponse](w, http.StatusBadRequest, &ErrorResponse{Error: message})
}

func encodeServerErrorResponse(w http.ResponseWriter) {
	encodeResponse[ErrorResponse](
		w,
		http.StatusInternalServerError,
		&ErrorResponse{Error: "server error"},
	)
}

type HashService interface {
	Encode(string) string
	Decode(string) string
}

type HashDataStore interface {
	CreateShortenedRecord(context.Context, string, string) (*ShortenedRecord, error)
	Get(context.Context, string) (*ShortenedRecord, error)
	CreateVisitRecord(context.Context, int) (*VisitRecord, error)
}

type ShortenedRecord struct {
	Id        int
	Encoded   string
	TargetURL string
	CreatedAt time.Time
}

type VisitRecord struct {
	Id        int
	ShortId   int
	CreatedAt time.Time
}

type HashCacheStore interface {
	Set(context.Context, string, *ShortenedRecord, time.Duration) error
	Get(context.Context, string) (*ShortenedRecord, error)
}
