package store

import "errors"

type User struct {
	First string
	Last  string
}

type InMemoryUserStore struct {
	Data map[string]*User
}

func NewInMemoryUserStore() *InMemoryUserStore {
	return &InMemoryUserStore{Data: make(map[string]*User)}
}

func (s *InMemoryUserStore) Create(first, last string) error {
	key := first + last
	if _, ok := s.Data[key]; ok {
		return errors.New("user already exists")
	}
	s.Data[key] = &User{
		First: first,
		Last:  last,
	}
	return nil
}
