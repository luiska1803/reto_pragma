CREATE TABLE IF NOT EXISTS events (
    id BIGSERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    price NUMERIC(18,6) NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    updated_by TEXT NOT NULL     
);

CREATE TABLE IF NOT EXISTS running_stats (
    id SMALLINT PRIMARY KEY DEFAULT 1,
    count BIGINT NOT NULL,
    mean DOUBLE PRECISION NOT NULL,
    min DOUBLE PRECISION NOT NULL,
    max DOUBLE PRECISION NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Inserta fila única si no existe 
INSERT INTO running_stats (id, count, mean, min, max)
SELECT 1, 0, 0.0, 'Infinity'::float8, '-Infinity'::float8
WHERE NOT EXISTS (SELECT 1 FROM running_stats WHERE id = 1);