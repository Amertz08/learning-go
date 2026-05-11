CREATE TABLE IF NOT EXISTS shortened_records (
    id BIGSERIAL PRIMARY KEY,
    encoded TEXT NOT NULL UNIQUE,
    target_url TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS link_visits (
    id BIGSERIAL PRIMARY KEY,
    short_id BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_visit_shortened
    FOREIGN KEY (short_id)
    REFERENCES shortened_records(id)
    ON DELETE CASCADE
);
