-- Mẫu bản ghi vi phạm data contract (Sprint 3, AI-5): collector ghi tối đa vài mẫu mỗi nguồn mỗi phút,
-- tool get_contract_violations của agent đọc lại để biết sai ở trường nào, mã nào.
CREATE TABLE IF NOT EXISTS contract_violation_samples (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source TEXT NOT NULL,
    symbol_native TEXT,
    event_id TEXT,
    ingest_minute TIMESTAMPTZ NOT NULL,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    errors JSONB NOT NULL,
    CONSTRAINT errors_is_array CHECK (jsonb_typeof(errors) = 'array'),
    CONSTRAINT unique_violation_sample UNIQUE (source, event_id, ingest_minute)
);

CREATE INDEX IF NOT EXISTS contract_violation_samples_source_time_idx
ON contract_violation_samples (source, ingest_minute DESC);
