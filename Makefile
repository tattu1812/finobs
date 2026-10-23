COMPOSE = docker compose --env-file .env -f infra/docker-compose.yml

.PHONY: help check up down stop restart ps logs

help:
	@echo "FinObs commands:"
	@echo "  make check   - Validate Docker Compose"
	@echo "  make up      - Start all services"
	@echo "  make down    - Stop and remove containers"
	@echo "  make stop    - Stop services"
	@echo "  make restart - Restart services"
	@echo "  make ps      - Show service status"
	@echo "  make logs    - Follow service logs"

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
