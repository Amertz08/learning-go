package cache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/redis/go-redis/v9"
	"github.come/Amertz08/learning-go/system_design/url_shortener/internal/handlers"
)

type RedisCache struct {
	rdb *redis.Client
}

func NewRedisCache(host, port string) *RedisCache {
	rdb := redis.NewClient(&redis.Options{
		Addr:     net.JoinHostPort(host, port),
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	r := &RedisCache{rdb: rdb}
	return r
}

func (r *RedisCache) Set(
	ctx context.Context,
	key string,
	value *handlers.ShortenedRecord,
	expiration time.Duration,
) error {
	var b bytes.Buffer
	if err := json.NewEncoder(&b).Encode(value); err != nil {
		return errors.New("failed to encode for cache")
	}
	cmd := r.rdb.Set(ctx, key, value, expiration)
	if cmd.Err() != nil {
		return errors.New("failed to set value in cache")
	}
	return nil
}

func (r *RedisCache) Get(ctx context.Context, key string) (*handlers.ShortenedRecord, error) {
	cmd := r.rdb.Get(ctx, key)
	if cmd.Err() != nil {
		return nil, errors.New(fmt.Sprintf("could not get key: %s", key))
	}

	byteData, err := cmd.Bytes()
	if err != nil {
		return nil, errors.New("could not get bytes")
	}
	var val handlers.ShortenedRecord
	if err = json.NewDecoder(bytes.NewReader(byteData)).Decode(&val); err != nil {
		return nil, errors.New(fmt.Sprintf("could not decode key: %s", key))
	}

	return &val, nil
}
