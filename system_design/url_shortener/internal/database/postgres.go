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
	args := pgx.NamedArgs{
		"encoded":    encoded,
		"target_url": targetUrl,
	}
	rows, err := s.db.Query(ctx, `
INSERT INTO shortened (encoded, target_url)
VALUES (@encoded, @target_url)
RETURNING id, created_at
;
`, args)
	if err != nil {
		return nil, err
	}
	record := &server.ShortenedRecord{
		Encoded:   encoded,
		TargetURL: targetUrl,
	}
	if err = rows.Scan(&record.Id, &record.CreatedAt); err != nil {
		return nil, err
	}
	return record, nil
}

func (s *PGDataStore) Get(context.Context, string) (*server.ShortenedRecord, error) {
	return nil, nil
}
func (s *PGDataStore) CreateVisitRecord(context.Context, int) (*server.VisitRecord, error) {
	return nil, nil
}
