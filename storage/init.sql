CREATE TABLE IF NOT EXISTS raw_readings (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  device_id TEXT NOT NULL,
  temperature DOUBLE PRECISION,
  humidity DOUBLE PRECISION,
  battery DOUBLE PRECISION,
  location TEXT,
  payload JSONB
);

CREATE INDEX IF NOT EXISTS idx_raw_readings_ts ON raw_readings(ts);
CREATE INDEX IF NOT EXISTS idx_raw_readings_device_ts ON raw_readings(device_id, ts);

CREATE TABLE IF NOT EXISTS agg_readings_minute (
  window_start TIMESTAMPTZ NOT NULL,
  window_end   TIMESTAMPTZ NOT NULL,
  device_id    TEXT NOT NULL,
  avg_temp     DOUBLE PRECISION,
  avg_humidity DOUBLE PRECISION,
  cnt          BIGINT,
  PRIMARY KEY (window_start, device_id)
);

CREATE TABLE IF NOT EXISTS alerts (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  device_id TEXT NOT NULL,
  rule TEXT NOT NULL,
  value DOUBLE PRECISION,
  threshold DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_alerts_ts ON alerts(ts);