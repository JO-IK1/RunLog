.PHONY: up down test check smoke fmt fmt-check
up:
	docker compose up --build -d

down:
	docker compose down

test:
	go test -race ./...

check: fmt-check
	go vet ./...
	go build ./...
	docker compose config --quiet

smoke:
	bash scripts/smoke.sh

fmt:
	gofmt -w cmd internal
	python3 -m yapf -i scripts/*.py

fmt-check:
	@test -z "$$(gofmt -l cmd internal)" || { gofmt -l cmd internal; exit 1; }
	python3 -m yapf --diff scripts/*.py
