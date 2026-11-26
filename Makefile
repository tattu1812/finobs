COMPOSE = docker compose --env-file .env -f infra/docker-compose.yml
# Mọi profile: dùng cho build/down/stop/ps/logs để không sót service nào.
COMPOSE_ALL = $(COMPOSE) --profile ops --profile timesync

# Ingest. Luôn chạy script bằng đường dẫn file: thư mục platform/ trùng tên module chuẩn.
VENV        := .venv
PY          ?= $(VENV)/bin/python
RAW         ?= data/raw
MINUTES     ?= 60
SOURCES     ?= binance,coinbase,kraken
VALIDATE_IN ?= $(if $(wildcard $(RAW)/*/*/*.jsonl.gz),$(RAW),tests/fixtures/raw)
# Ngày file chính thức Binance cần nạp (mặc định: hôm qua theo UTC)
DATE        ?= $(shell date -u -d yesterday +%F 2>/dev/null)

# Chỉ tự dựng venv khi dùng venv mặc định (CI truyền PY=python)
ifeq ($(PY),$(VENV)/bin/python)
PY_DEP := $(VENV)/.installed
endif

TRINO = docker exec finobs-trino trino --output-format ALIGNED --execute

.PHONY: help check build up down stop restart ps logs venv clock-check record analyze \
	validate-contracts validate-scenarios test test-agent test-observability test-platform test-reconcile \
	lineage bronze-check official-check load-official up-core timesync reprocess gold-check \
	completeness-check bench test-spark reconcile replay

help:
	@echo "FinObs commands:"
	@echo "  make check   - Validate Docker Compose"
	@echo "  make build   - Build image finobs-app, finobs-spark, finobs-airflow"
	@echo "  make up      - Build (nếu cần) và chạy lõi + profile ops (Airflow, Marquez, Grafana)"
	@echo "  make up-core - Chỉ phần lõi: thu, bronze/silver/gold, metric, alert (máy ít RAM)"
	@echo "  make timesync - Đồng bộ giờ máy ảo Docker Desktop bằng NTP (không dùng trên máy chủ Linux)"
	@echo "  make down    - Stop and remove containers (giữ volume)"
	@echo "  make stop    - Stop services"
	@echo "  make restart - Restart services"
	@echo "  make ps      - Show service status"
	@echo "  make logs    - Follow service logs"
	@echo ""
	@echo "Pipeline:"
	@echo "  make bronze-check       - Trino: dữ liệu live mới nhất trên bronze.trades_raw"
	@echo "  make load-official      - Nạp file chính thức Binance ngày DATE=$(DATE) (trong container Airflow)"
	@echo "  make official-check     - Trino: số trade và dãy ID của bronze.binance_official_trades"
	@echo "  make reprocess          - Dừng silver-gold, tính lại silver/gold từ đầu bronze (FROM=, TO= tùy chọn)"
	@echo "  make gold-check         - Gold có tính đúng không: đếm lại từ silver, phải khớp 100%"
	@echo "  make completeness-check - Stream có đủ không: gold Binance so với file chính thức ngày DATE (chỉ số C2)"
	@echo "  make bench              - Chạy injector trên mọi scenario (ra bench/, BENCH_DSN= để ghi bench.scenarios)"
	@echo "  make reconcile          - Đối soát D+1 stream vs file chính thức (DATE=$(DATE))"
	@echo ""
	@echo "Ingest (chạy trên máy, ra file):"
	@echo "  make venv               - Tạo .venv và cài requirements-ingest.txt + requirements-test.txt"
	@echo "  make clock-check        - Đo lệch đồng hồ (dừng nếu > 250 ms)"
	@echo "  make record             - clock-check rồi thu MINUTES=$(MINUTES) phút từ SOURCES=$(SOURCES)"
	@echo "  make analyze            - Bảng đo + biểu đồ vào reports/"
	@echo "  make validate-contracts - Validate VALIDATE_IN với platform/contracts + ca lỗi cố ý"
	@echo "  make validate-scenarios - Kiểm tra scenario v2, scenario v1 cũ, ca lỗi cố ý + unit test"
	@echo ""
	@echo "Test:"
	@echo "  make test               - Chạy mọi unit test"
	@echo "  make test-platform      - Producer (Kafka writer) + bộ nạp file chính thức"
	@echo "  make test-observability - Collector trade_count, freshness, vòng chạy liên tục"
	@echo "  make test-agent         - 5 tool agent, RCA heuristic"
	@echo "  make test-spark         - Logic silver/gold trên Spark local (trong image finobs-spark)"
	@echo "  make lineage            - In đồ thị lineage (MARQUEZ_URL=http://localhost:5000 để dùng Marquez thật)"

check:
	$(COMPOSE) config --quiet

build:
	$(COMPOSE_ALL) build

up:
	$(COMPOSE) --profile ops up -d --build

up-core:
	$(COMPOSE) up -d --build

timesync:
	$(COMPOSE) --profile timesync up -d timesync

down:
	$(COMPOSE_ALL) down

stop:
	$(COMPOSE_ALL) stop

restart:
	$(COMPOSE) restart

ps:
	$(COMPOSE_ALL) ps

logs:
	$(COMPOSE_ALL) logs -f --tail=100

venv: $(VENV)/.installed

$(VENV)/.installed: requirements-ingest.txt requirements-test.txt
	test -x $(VENV)/bin/python || python3 -m venv $(VENV)
	$(VENV)/bin/pip install -q -r requirements-ingest.txt -r requirements-test.txt
	touch $@

clock-check: $(PY_DEP)
	$(PY) platform/ingest/clock_check.py --label make-record --out reports/clock_check.jsonl

record: clock-check
	$(PY) platform/ingest/record.py --minutes $(MINUTES) --sources $(SOURCES) --symbols config/symbols.json --out $(RAW)

analyze: $(PY_DEP)
	$(PY) platform/ingest/analyze.py --in $(RAW) --out reports

validate-contracts: $(PY_DEP)
	$(PY) platform/contracts/validate.py --in $(VALIDATE_IN)
	$(PY) platform/contracts/validate.py --in tests/fixtures/invalid --expect-invalid --show 0
	$(PY) -c "import yaml, pathlib; [yaml.safe_load(p.read_text(encoding='utf-8')) for p in sorted(pathlib.Path('config/contracts').glob('*.contract.yaml'))]"

test: test-platform test-observability test-agent test-reconcile test-sprint4

test-platform: $(PY_DEP)
	$(PY) -m unittest tests.test_producer_sinks tests.test_kraken_adapter tests.test_binance_official \
	  tests.test_injector -v

test-agent: $(PY_DEP)
	$(PY) -m unittest tests.test_agent_tools tests.test_metric_store tests.test_rca_heuristic -v

test-observability: $(PY_DEP)
	$(PY) -m unittest tests.test_trade_count_collector tests.test_freshness_collector \
	  tests.test_trading_calendar tests.test_pipeline_health -v

# Logic silver/gold cần Spark: chạy trong image finobs-spark (có pyspark), mount repo chỉ đọc.
test-spark:
	docker run --rm -v "$(CURDIR):/repo:ro" -w /repo -e PYTHONDONTWRITEBYTECODE=1 finobs-spark:local \
	  sh -c 'PYTHONPATH=/opt/spark/python:$$(ls /opt/spark/python/lib/py4j-*.zip) python3 -m unittest tests.test_silver_gold -v'

test-reconcile: $(PY_DEP)
	$(PY) -m unittest tests.test_gap_detection tests.test_reconcile -v

test-sprint4: $(PY_DEP)
	$(PY) -m unittest tests.test_sketch_profiler tests.test_slo_engine tests.test_rca_incidents tests.test_eval_v2 -v

validate-slo: $(PY_DEP)
	$(PY) -c "from observability.slo.slo_engine import validate_slo_file; validate_slo_file(); print('SLO YAML OK')"

lineage: $(PY_DEP)
	$(PY) -m agent.tools.lineage_graph --alerts $(if $(MARQUEZ_URL),--marquez-url $(MARQUEZ_URL))

validate-scenarios: $(PY_DEP)
	$(if $(PY_DEP),$(VENV)/bin/pip install -q -r requirements-eval.txt)
	$(PY) evaluation/scenarios/validate.py
	$(PY) evaluation/scenarios/validate.py --in tests/fixtures/scenarios_v1
	$(PY) evaluation/scenarios/validate.py --in tests/fixtures/scenarios_invalid --expect-invalid
	$(PY) -m unittest discover -s evaluation/scenarios -p test_validate.py

bronze-check:
	$(TRINO) "SELECT source, count(*) AS rows, max(ingest_ts) AS newest_ingest, \
	  date_diff('second', max(bronze_ts), current_timestamp) AS bronze_age_s \
	  FROM iceberg.bronze.trades_raw GROUP BY source ORDER BY source"

load-official:
	docker exec finobs-airflow-scheduler python /opt/finobs/platform/batch/binance_official.py --date $(DATE) \
	  --symbols /opt/finobs/config/symbols.json

official-check:
	$(TRINO) "SELECT file_date, symbol, count(*) AS rows, max(trade_id) - min(trade_id) + 1 AS id_span, \
	  count(DISTINCT checksum) AS files FROM iceberg.bronze.binance_official_trades \
	  GROUP BY 1, 2 ORDER BY 1 DESC, 3 DESC"

# Tính lại silver/gold từ đầu bronze (cùng code với chế độ stream, MERGE idempotent).
reprocess:
	$(COMPOSE) stop silver-gold
	$(COMPOSE) run --rm --no-deps silver-gold sh -c '/opt/spark/bin/spark-submit \
	  --master spark://spark-master:7077 --conf spark.driver.host=$$(hostname -i) \
	  --conf spark.driver.memory=768m --conf spark.executor.memory=1g --conf spark.cores.max=1 \
	  --conf spark.sql.shuffle.partitions=4 /opt/finobs/platform/streaming/silver_gold.py --mode rebuild \
	  $(if $(FROM),--from $(FROM)) $(if $(TO),--to $(TO))'
	$(COMPOSE) start silver-gold

# Gold có tính đúng không: đếm lại từ silver, phải khớp 100% (mismatched_minutes = 0).
gold-check:
	$(TRINO) "WITH s AS (SELECT source, symbol_std, date_trunc('minute', event_ts) AS minute, count(*) AS n, \
	  sum(qty) AS volume FROM iceberg.silver.trades GROUP BY 1, 2, 3) \
	  SELECT count(*) AS minutes_checked, \
	  count_if(g.trade_count IS DISTINCT FROM s.n OR g.volume IS DISTINCT FROM s.volume) AS mismatched_minutes \
	  FROM s FULL JOIN iceberg.gold.ohlcv_1m g \
	  ON g.source = s.source AND g.symbol_std = s.symbol_std AND g.minute = s.minute \
	  WHERE coalesce(g.minute, s.minute) < current_timestamp - INTERVAL '15' MINUTE"

# Stream có đủ không (chỉ số C2, S4): gold Binance so với file chính thức theo giờ của ngày DATE.
completeness-check:
	$(TRINO) "WITH o AS (SELECT symbol, date_trunc('hour', trade_ts) AS hour, count(*) AS official \
	  FROM iceberg.bronze.binance_official_trades WHERE file_date = DATE '$(DATE)' GROUP BY 1, 2), \
	  s AS (SELECT replace(symbol_canonical, '-', '') AS symbol, date_trunc('hour', minute) AS hour, \
	  sum(trade_count) AS stream FROM iceberg.gold.ohlcv_1m WHERE source = 'binance' \
	  AND minute >= TIMESTAMP '$(DATE) 00:00:00 UTC' AND minute < TIMESTAMP '$(DATE) 00:00:00 UTC' + INTERVAL '1' DAY \
	  GROUP BY 1, 2) \
	  SELECT o.symbol, count(*) AS hours, sum(o.official) AS official, sum(coalesce(s.stream, 0)) AS stream, \
	  round(100.0 * sum(coalesce(s.stream, 0)) / sum(o.official), 3) AS completeness_pct \
	  FROM o LEFT JOIN s ON s.symbol = o.symbol AND s.hour = o.hour GROUP BY 1 ORDER BY 1"

bench: $(PY_DEP)
	$(PY) evaluation/injector/inject.py evaluation/scenarios/S0*.yaml --out bench $(if $(BENCH_DSN),--dsn $(BENCH_DSN))

reconcile: $(PY_DEP)
	$(PY) observability/reconcile/reconcile.py --date $(DATE) \
	  $(if $(SYMBOLS),--symbols $(SYMBOLS)) \
	  $(if $(FROM),--from $(FROM)) $(if $(TO),--to $(TO)) \
	  $(if $(FIXTURE_DIR),--fixture-dir $(FIXTURE_DIR))

replay: $(PY_DEP)
	$(PY) infra/replay/replayer.py --in $(or $(IN),tests/fixtures/raw) \
	  --speed $(or $(SPEED),max) --namespace $(or $(NAMESPACE),replay) \
	  $(if $(SYMBOLS),--symbols $(SYMBOLS)) $(if $(NUM_SYMBOLS),--num-symbols $(NUM_SYMBOLS)) \
	  $(if $(FROM),--from $(FROM)) $(if $(TO),--to $(TO)) \
	  $(if $(SEED),--seed $(SEED)) $(if $(OUT),--out $(OUT))
