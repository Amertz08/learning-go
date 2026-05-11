package database

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.come/Amertz08/learning-go/system_design/url_shortener/internal/server"
)

type PGDataStore struct {
	db *pgx.Conn
}

func NewPGDataStore(db *pgx.Conn) *PGDataStore {
	return &PGDataStore{db: db}
}

func (s *PGDataStore) CreateShortenedRecord(
	ctx context.Context,
	encoded, targetUrl string,
) (*server.ShortenedRecord, error) {
	return nil, nil
}
