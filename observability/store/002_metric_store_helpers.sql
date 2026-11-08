CREATE INDEX IF NOT EXISTS metric_observations_name_time_idx
ON metric_observations (metric_name, observed_at DESC);

CREATE INDEX IF NOT EXISTS metric_observations_dimensions_idx
ON metric_observations USING GIN (dimensions jsonb_path_ops);

CREATE OR REPLACE FUNCTION upsert_metric_observation(
    p_metric_name TEXT,
    p_dimensions JSONB,
    p_observed_at TIMESTAMPTZ,
    p_window_start TIMESTAMPTZ,
    p_window_end TIMESTAMPTZ,
    p_value NUMERIC,
    p_status TEXT,
    p_sample_count BIGINT
) RETURNS BIGINT
LANGUAGE SQL
AS $$
    INSERT INTO metric_observations (
        metric_name,
        dimensions,
        observed_at,
        window_start,
        window_end,
        value,
        status,
        sample_count
    )
    VALUES (
        p_metric_name,
        p_dimensions,
        p_observed_at,
        p_window_start,
        p_window_end,
        p_value,
        p_status,
        p_sample_count
    )
    ON CONFLICT ON CONSTRAINT unique_metric_observation
    DO UPDATE SET
        window_start = EXCLUDED.window_start,
        window_end = EXCLUDED.window_end,
        value = EXCLUDED.value,
        status = EXCLUDED.status,
        sample_count = EXCLUDED.sample_count,
        recorded_at = now()
    RETURNING id;
$$;