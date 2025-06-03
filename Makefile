.PHONY: build run test migrate clean

build:
	go build -o nebula-conduit ./cmd/server

run: build
	./nebula-conduit

test:
	go test ./...

migrate:
	sqlite3 data/nebula.db < migrations/001_init_schema.up.sql

docker-build:
	docker-compose build

docker-run:
	docker-compose up

clean:
	rm -f nebula-conduit
