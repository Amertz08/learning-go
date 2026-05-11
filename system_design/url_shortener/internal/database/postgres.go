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

	record := &server.ShortenedRecord{
		Encoded:   encoded,
		TargetURL: targetUrl,
	}

	err := s.db.QueryRow(ctx, `
INSERT INTO public.shortened_records (encoded, target_url)
VALUES ($1, $2)
RETURNING id, created_at
;
`, encoded, targetUrl).Scan(&record.Id, &record.CreatedAt)
	if err != nil {
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
