package internal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.come/Amertz08/learning-go/system_design/url_shortener/internal/server"
)

type FakeDataStore struct {
	Data              map[string]*server.ShortenedRecord
	HasCreateShortErr bool
	HasGetError       bool
	Visits            map[int]*server.VisitRecord
}

func NewFakeDataStore() *FakeDataStore {
	return &FakeDataStore{
		Data:   make(map[string]*server.ShortenedRecord),
		Visits: make(map[int]*server.VisitRecord),
	}
}

func (f *FakeDataStore) CreateShortenedRecord(
	ctx context.Context,
	shortened, original string,
) (*server.ShortenedRecord, error) {
	if f.HasCreateShortErr {
		return nil, errors.New("failed to create record")
	}
	f.Data[shortened] = &server.ShortenedRecord{
		Id:        1,
		Encoded:   shortened,
		TargetURL: original,
		CreatedAt: time.Now(),
	}
	return f.Data[shortened], nil
}

func (f *FakeDataStore) Get(ctx context.Context, key string) (*server.ShortenedRecord, error) {
	val, _ := f.Data[key]
	if f.HasGetError {
		return nil, fmt.Errorf("error getting db key: %s", key)
	}
	return val, nil
}

func (f *FakeDataStore) CreateVisitRecord(ctx context.Context, shortId int) (*server.VisitRecord, error) {
	v := &server.VisitRecord{
		Id:        1,
		ShortId:   shortId,
		CreatedAt: time.Now(),
	}
	f.Visits[v.Id] = v
	return v, nil
}
