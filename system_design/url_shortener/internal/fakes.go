package internal

import (
	"errors"
	"fmt"
	"time"

	"github.come/Amertz08/learning-go/system_design/url_shortener/internal/handlers"
)

type FakeHasher struct {
}

func NewFakeHasher() *FakeHasher {
	return &FakeHasher{}
}

func (f *FakeHasher) Encode(input string) string { return input + "+hello" }
func (f *FakeHasher) Decode(input string) string { return "" }

type FakeDataStore struct {
	Data              map[string]*handlers.ShortenedRecord
	HasCreateShortErr bool
	HasGetError       bool
	Visits            map[int]*handlers.VisitRecord
}

func NewFakeDataStore() *FakeDataStore {
	return &FakeDataStore{
		Data:   make(map[string]*handlers.ShortenedRecord),
		Visits: make(map[int]*handlers.VisitRecord),
	}
}

func (f *FakeDataStore) CreateShortenedRecord(
	shortened, original string,
) (*handlers.ShortenedRecord, error) {
	if f.HasCreateShortErr {
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

func (f *FakeDataStore) Get(key string) (*handlers.ShortenedRecord, error) {
	val, _ := f.Data[key]
	if f.HasGetError {
		return nil, fmt.Errorf("error getting db key: %s", key)
	}
	return val, nil
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
