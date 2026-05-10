package handlers

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"
)

func ShortenHandler(
	hasher HashService,
	store HashDataStore,
	cache HashCacheStore,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		data, err := decodeRequest[ShortenRequest](r.Body)

		if err != nil {
			encodeResponse(w, http.StatusInternalServerError, nil)
			return
		}
		encoded := hasher.Encode(data.URL)

		if err = store.Create(encoded, data.URL); err != nil {
			encodeResponse(w, http.StatusBadRequest, nil)
		}

		if err = cache.Set(data.URL, encoded, 30*time.Minute); err != nil {
			encodeResponse(w, http.StatusInternalServerError, nil)
		}

		encodeResponse(w, http.StatusOK, &ShortenedResponse{URL: encoded})
	}
}

type ShortenRequest struct {
	URL string `json:"url"`
}
type ShortenedResponse struct {
	URL string `json:"url"`
}

func decodeRequest[T any](r io.ReadCloser) (*T, error) {
	var data T
	if err := json.NewDecoder(r).Decode(&data); err != nil {
		return nil, errors.New("could not decode request")
	}
	return &data, nil
}

func encodeResponse(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if err := json.NewEncoder(w).Encode(data); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
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
}
