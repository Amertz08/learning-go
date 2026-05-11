package internal

import (
	"errors"
	"time"

	"github.come/Amertz08/learning-go/system_design/url_shortener/internal/handlers"
)

type FakeHasher struct {
}

func (f *FakeHasher) Encode(input string) string { return input + "+hello" }
func (f *FakeHasher) Decode(input string) string { return "" }

type FakeDataStore struct {
	Data           map[string]*handlers.ShortenedRecord
	HasCreateError bool
	Visits         map[int]*handlers.VisitRecord
}

func (f *FakeDataStore) CreateShortenedRecord(
	shortened, original string,
) (*handlers.ShortenedRecord, error) {
	if f.HasCreateError {
		return nil, errors.New("failed to create record")
	}
	f.Data[shortened] = &handlers.ShortenedRecord{
		Id:        1,
		Encoded:   shortened,
		TargetURL: original,
		CreatedAt: time.Now(),
	}
	return f.Data[shortened], nil
}

func (f *FakeDataStore) Get(key string) (*handlers.ShortenedRecord, bool) {
	val, ok := f.Data[key]
	return val, ok
}

func (f *FakeDataStore) CreateVisitRecord(shortId int) (*handlers.VisitRecord, error) {
	v := &handlers.VisitRecord{
		Id:        1,
		ShortId:   shortId,
		CreatedAt: time.Now(),
	}
	f.Visits[v.Id] = v
	return v, nil
}

type FakeCacheStore struct {
	Cache       map[string]*handlers.ShortenedRecord
	HasSetError bool
}

func (f *FakeCacheStore) Set(
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

func (f *FakeCacheStore) Get(key string) (*handlers.ShortenedRecord, bool) {
	val, ok := f.Cache[key]
	return val, ok
}
