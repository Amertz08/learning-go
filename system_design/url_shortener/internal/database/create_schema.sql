CREATE TABLE IF NOT EXISTS shortened_records (
    id BIGSERIAL PRIMARY KEY,
    encoded TEXT NOT NULL UNIQUE,
    target_url TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
