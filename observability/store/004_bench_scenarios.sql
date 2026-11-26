-- Đáp án FinObs-Bench (đặc tả Bảng 10: bench.scenarios; Sprint 3, EV-5).
-- Mỗi lần injector chạy một scenario ghi một dòng: tiêm gì, vào đâu, lúc nào, với seed nào.
-- Cột injected_from/injected_to là thời điểm tiêm THẬT (để đo MTTD), khác khoảng dự kiến trong YAML.
CREATE SCHEMA IF NOT EXISTS bench;

CREATE TABLE IF NOT EXISTS bench.scenarios (
    run_id TEXT PRIMARY KEY,
    scenario_id TEXT NOT NULL,
    scenario_version INT NOT NULL,
    fault_type TEXT NOT NULL,
    is_incident BOOLEAN NOT NULL,
    injector TEXT,
    target_node TEXT,
    root_cause TEXT,
    affected_assets JSONB NOT NULL DEFAULT '[]'::jsonb,
    params JSONB NOT NULL DEFAULT '{}'::jsonb,
    seed BIGINT,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    injected_from TIMESTAMPTZ,
    injected_to TIMESTAMPTZ,
    records_in BIGINT NOT NULL,
    records_out BIGINT NOT NULL,
    records_affected BIGINT NOT NULL,
    output_sha256 TEXT NOT NULL,
    output_target TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS bench_scenarios_scenario_idx ON bench.scenarios (scenario_id, created_at DESC);
