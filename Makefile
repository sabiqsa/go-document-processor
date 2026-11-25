TEST_DATABASE_URL ?= postgres://postgres:postgres@localhost:5432/docproc_test?sslmode=disable

.PHONY: build test test-race test-integration bench vet run db-up db-down

build:
	go build ./...

vet:
	go vet ./...

test:
	go test ./...

# Required by the task: pass repeatedly with the race detector.
test-race:
	go test -race -count=10 ./...

test-integration:
	TEST_DATABASE_URL="$(TEST_DATABASE_URL)" go test -race -count=1 ./...

bench:
	go test -run '^$$' -bench . -benchmem ./docproc/

run:
	go run ./cmd/docproc-server

db-up:
	docker compose up -d --wait postgres

db-down:
	docker compose down -v
