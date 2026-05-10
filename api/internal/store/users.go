package store

import (
	"context"
	"errors"

	"github.com/google/uuid"
)

type User struct {
	Id    string
	First string
	Last  string
}

type InMemoryUserStore struct {
	Data map[string]*User
}

func NewInMemoryUserStore() *InMemoryUserStore {
	return &InMemoryUserStore{Data: make(map[string]*User)}
}

func (s *InMemoryUserStore) Create(ctx context.Context, first, last string) (string, error) {
	key := first + last
	if _, ok := s.Data[key]; ok {
		return "", errors.New("user already exists")
	}
	id, _ := uuid.NewUUID()
	s.Data[key] = &User{
		Id:    id.String(),
		First: first,
		Last:  last,
	}
	return id.String(), nil
}
