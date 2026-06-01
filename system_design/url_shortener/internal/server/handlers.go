package server

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/labstack/echo/v5"
)

// TODO: err empty response is unused now
var (
	ErrEmptyRequest          = errors.New("empty request")
	ErrCouldNotDecodeRequest = errors.New("could not decode request")
)

// TODO: cache expiry via config
// TODO: throttle creation

func ShortenHandler(
	logger *slog.Logger,
	hasher HashService,
	store HashDataStore,
	cache HashCacheStore,
) echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		data, err := decodeRequest[ShortenRequest](c)
		if err != nil {
			logger.Error("could not decode request", slog.Any("error", err))
			return encodeServerErrorResponse(c)
		}

		if err = c.Validate(data); err != nil {
			return c.JSON(http.StatusBadRequest, &ErrorResponse{Error: "invalid parameters"})
		}

		encoded := hasher.Encode(data.URL)

		// TODO: check cache if exists

		shortRecord, err := store.CreateShortenedRecord(ctx, encoded, data.URL)
		if err != nil {
			logger.Error("could not save shortened link", slog.Any("error", err))
			return encodeServerErrorResponse(c)
		}

		if err = cache.Set(ctx, encoded, shortRecord, 30*time.Minute); err != nil {
			logger.Error("could not cache value", slog.Any("error", err))
			return encodeServerErrorResponse(c)
		}

		// TODO: shorten prefix should be a config
		return c.JSON(http.StatusOK, &ShortenedResponse{URL: "http://localhost:8080/v/" + encoded})
	}
}

type ShortenRequest struct {
	URL string `json:"url" validate:"required"`
}

type ShortenedResponse struct {
	URL string `json:"url"`
}

func VisitHandler(logger *slog.Logger, store HashDataStore, cache HashCacheStore) echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		hash := c.Param("short_hash")

		shortRecord, err := cache.Get(ctx, hash)
		if err != nil {
			logger.Error("error getting hash from cache", slog.Any("error", err))
			return encodeServerErrorResponse(c)
		}
		if shortRecord == nil {
			shortRecord, err = store.Get(ctx, hash)
			if err != nil {
				logger.Error("error getting hash from DB", slog.Any("error", err))
				return encodeServerErrorResponse(c)
			}
			if shortRecord == nil {
				logger.Warn("not found", slog.Any("hash", hash))
				return echo.NewHTTPError(http.StatusNotFound, "record not found")
			}
			if err = cache.Set(ctx, hash, shortRecord, 30*time.Minute); err != nil {
				logger.Error("error setting the cache", slog.Any("error", err))
				return encodeServerErrorResponse(c)
			}
		}

		_, err = store.CreateVisitRecord(ctx, shortRecord.Id)
		if err != nil {
			logger.Error("error creating visit", slog.Any("error", err))
			return encodeServerErrorResponse(c)
		}

		return c.Redirect(http.StatusFound, shortRecord.TargetURL)
	}
}

func decodeRequest[T any](c *echo.Context) (*T, error) {
	var data T
	if err := c.Bind(&data); err != nil {
		return nil, ErrCouldNotDecodeRequest
	}
	return &data, nil
}

type ErrorResponse struct {
	Error string `json:"error"`
}

func encodeServerErrorResponse(c *echo.Context) error {
	return c.JSON(http.StatusInternalServerError, &ErrorResponse{Error: "server error"})
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

// ShortenedRecord is the mapping of the target URL to the encoded value
type ShortenedRecord struct {
	Id        int
	Encoded   string
	TargetURL string
	CreatedAt time.Time
}

// VisitRecord captures an individual visit and redirect to the target.
// In theory, you'd likely want IP address and other attributes in the record
type VisitRecord struct {
	Id        int
	ShortId   int
	CreatedAt time.Time
}

type HashCacheStore interface {
	Set(context.Context, string, *ShortenedRecord, time.Duration) error
	Get(context.Context, string) (*ShortenedRecord, error)
}
