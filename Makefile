# Recipes must run in both sh and Windows cmd: no shell loops, no VAR=value prefixes.
SERVICES := api-gateway storage-service analytics-service report-service
API ?= http://localhost:8080
DEV := -f docker-compose.yml -f docker-compose.dev.yml
ifeq ($(OS),Windows_NT)
PYTHON ?= python
else
PYTHON ?= python3
endif

.PHONY: up up-dev down reset ps logs test test-api simulate buckets jobs \
        k8s-images k8s-up k8s-status k8s-forward k8s-restart k8s-down

.env:
	$(PYTHON) -c "import shutil; shutil.copyfile('.env.example', '.env')"

# Docker Compose
up: .env
	docker compose up --build -d

up-dev: .env
	docker compose $(DEV) up --build -d

down:
	docker compose down

reset:
	docker compose down -v

ps:
	docker compose ps

logs:
	docker compose logs -f

# Tests and traffic
test:
	cd services && pytest

test-api: export API_BASE_URL := $(API)
test-api:
	cd tests && pytest

simulate:
	$(PYTHON) scripts/measurement_simulator.py --api $(API)

# Evidence helpers
buckets:
	docker compose exec -T api-gateway python - < scripts/list_buckets.py

jobs:
	docker compose exec redis redis-cli KEYS "job:*"

# Kubernetes (Minikube)
k8s-images: .env
	docker compose build
	$(foreach image,$(SERVICES) garage-init,minikube image load aic/$(image):local &&) echo images loaded

k8s-up:
	kubectl apply -f k8s/manifests/

k8s-status:
	kubectl get pods,deployments,services,pvc,jobs -n aic

k8s-forward:
	kubectl port-forward -n aic svc/api-gateway 18080:8080

k8s-restart:
	kubectl rollout restart deployment -n aic $(SERVICES)

k8s-down:
	kubectl delete namespace aic
