package cache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.come/Amertz08/learning-go/system_design/url_shortener/internal/server"
)

type FakeCacheStore struct {
	Cache       map[string]*server.ShortenedRecord
	HasSetError bool
	HasGetError bool
}

func NewFakeCacheStore() *FakeCacheStore {
	return &FakeCacheStore{Cache: make(map[string]*server.ShortenedRecord)}
}

func (f *FakeCacheStore) Set(
	ctx context.Context,
	key string,
	value *server.ShortenedRecord,
	expiration time.Duration,
) error {
	if f.HasSetError {
		return errors.New("error setting cache")
	}
	f.Cache[key] = value
	return nil
}

func (f *FakeCacheStore) Get(ctx context.Context, key string) (*server.ShortenedRecord, error) {
	val, _ := f.Cache[key]
	if f.HasGetError {
		return nil, fmt.Errorf("error getting key: %s", key)
	}
	return val, nil
}
