package cache

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"time"

	"github.com/redis/go-redis/v9"
	"github.come/Amertz08/learning-go/system_design/url_shortener/internal/server"
)

type redisCache struct {
	rdb *redis.Client
}

func NewRedisCache(host, port string) *redisCache {
	rdb := redis.NewClient(&redis.Options{
		Addr:     net.JoinHostPort(host, port),
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	r := &redisCache{rdb: rdb}
	return r
}

func (r *redisCache) Close() error {
	return r.rdb.Close()
}

func (r *redisCache) Set(
	ctx context.Context,
	key string,
	value *server.ShortenedRecord,
	expiration time.Duration,
) error {
	var b bytes.Buffer
	if err := json.NewEncoder(&b).Encode(value); err != nil {
		return err
	}
	cmd := r.rdb.Set(ctx, key, b.String(), expiration)
	if cmd.Err() != nil {
		return cmd.Err()
	}
	return nil
}

func (r *redisCache) Get(ctx context.Context, key string) (*server.ShortenedRecord, error) {
	cmd := r.rdb.Get(ctx, key)
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}

	byteData, err := cmd.Bytes()
	if err != nil {
		return nil, err
	}
	var val server.ShortenedRecord
	if err = json.NewDecoder(bytes.NewReader(byteData)).Decode(&val); err != nil {
		return nil, err
	}

	return &val, nil
}
