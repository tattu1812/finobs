CREATE TABLE IF NOT EXISTS metric_observations (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    metric_name TEXT NOT NULL,
    dimensions JSONB NOT NULL DEFAULT '{}'::jsonb,

    observed_at TIMESTAMPTZ NOT NULL,
    window_start TIMESTAMPTZ,
    window_end TIMESTAMPTZ,

    value NUMERIC(38, 18),
    status TEXT NOT NULL,
    sample_count BIGINT,

    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT dimensions_is_object
        CHECK (jsonb_typeof(dimensions) = 'object'),

    CONSTRAINT valid_window
        CHECK (
            (window_start IS NULL AND window_end IS NULL)
            OR
            (window_start IS NOT NULL
             AND window_end IS NOT NULL
             AND window_start < window_end)
        ),

    CONSTRAINT valid_status
        CHECK (status IN (
            'ok',
            'no_data',
            'source_unavailable',
            'insufficient_data',
            'error'
        )),

    CONSTRAINT value_matches_status
        CHECK (
            (status = 'ok' AND value IS NOT NULL)
            OR
            (status <> 'ok' AND value IS NULL)
        ),

    CONSTRAINT nonnegative_sample_count
        CHECK (sample_count IS NULL OR sample_count >= 0),

    CONSTRAINT unique_metric_observation
        UNIQUE (metric_name, dimensions, observed_at)
);