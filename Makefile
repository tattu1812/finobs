COMPOSE = docker compose --env-file .env -f infra/docker-compose.yml

# Ingest. Luôn chạy script bằng đường dẫn file: thư mục platform/ trùng tên module chuẩn.
VENV        := .venv
PY          ?= $(VENV)/bin/python
RAW         ?= data/raw
MINUTES     ?= 60
SOURCES     ?= binance,coinbase
VALIDATE_IN ?= $(if $(wildcard $(RAW)/*/*/*.jsonl.gz),$(RAW),tests/fixtures/raw)

# Chỉ tự dựng venv khi dùng venv mặc định (CI truyền PY=python)
ifeq ($(PY),$(VENV)/bin/python)
PY_DEP := $(VENV)/.installed
endif

.PHONY: help check up down stop restart ps logs venv clock-check record analyze validate-contracts validate-scenarios test-agent lineage

help:
	@echo "FinObs commands:"
	@echo "  make check   - Validate Docker Compose"
	@echo "  make up      - Start all services"
	@echo "  make down    - Stop and remove containers"
	@echo "  make stop    - Stop services"
	@echo "  make restart - Restart services"
	@echo "  make ps      - Show service status"
	@echo "  make logs    - Follow service logs"
	@echo ""
	@echo "Ingest:"
	@echo "  make venv               - Tạo .venv và cài requirements-ingest.txt"
	@echo "  make clock-check        - Đo lệch đồng hồ (dừng nếu > 250 ms)"
	@echo "  make record             - clock-check rồi thu MINUTES=$(MINUTES) phút từ SOURCES=$(SOURCES)"
	@echo "  make analyze            - Bảng đo + biểu đồ vào reports/"
	@echo "  make validate-contracts - Validate VALIDATE_IN với platform/contracts + ca lỗi cố ý"
	@echo "  make validate-scenarios - Kiểm tra evaluation/scenarios/*.yaml + ca lỗi cố ý"
	@echo ""
	@echo "Agent:"
	@echo "  make test-agent         - Test 5 tool agent trên dữ liệu giả + client Marquez"
	@echo "  make lineage            - In đồ thị lineage (MARQUEZ_URL=http://localhost:5000 để dùng Marquez thật)"

check:
	$(COMPOSE) config --quiet

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

stop:
	$(COMPOSE) stop

restart:
	$(COMPOSE) restart

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=100

venv: $(VENV)/.installed

$(VENV)/.installed: requirements-ingest.txt
	test -x $(VENV)/bin/python || python3 -m venv $(VENV)
	$(VENV)/bin/pip install -q -r requirements-ingest.txt
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

test-agent: $(PY_DEP)
	$(PY) -m unittest tests.test_agent_tools -v

lineage: $(PY_DEP)
	$(PY) -m agent.tools.lineage_graph --alerts $(if $(MARQUEZ_URL),--marquez-url $(MARQUEZ_URL))

validate-scenarios: $(PY_DEP)
	$(if $(PY_DEP),$(VENV)/bin/pip install -q -r requirements-eval.txt)
	$(PY) evaluation/scenarios/validate.py
	$(PY) evaluation/scenarios/validate.py --in tests/fixtures/scenarios_invalid --expect-invalid
