package cache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.come/Amertz08/learning-go/system_design/url_shortener/internal/handlers"
)

type FakeCacheStore struct {
	Cache       map[string]*handlers.ShortenedRecord
	HasSetError bool
	HasGetError bool
}

func NewFakeCacheStore() *FakeCacheStore {
	return &FakeCacheStore{Cache: make(map[string]*handlers.ShortenedRecord)}
}

func (f *FakeCacheStore) Set(
	ctx context.Context,
	key string,
	value *handlers.ShortenedRecord,
	expiration time.Duration,
) error {
	if f.HasSetError {
		return errors.New("error setting cache")
	}
	f.Cache[key] = value
	return nil
}

func (f *FakeCacheStore) Get(ctx context.Context, key string) (*handlers.ShortenedRecord, error) {
	val, _ := f.Cache[key]
	if f.HasGetError {
		return nil, fmt.Errorf("error getting key: %s", key)
	}
	return val, nil
}
