.DEFAULT_GOAL= build

.PHONY: run fmt vet build clean test swagger

fmt: 
	go fmt ./...

vet: fmt
	go vet ./...

swagger:
	swag init -g main.go --parseDependency --parseInternal -o ./docs

build: vet swagger
	go build -o bin/async-job-queue .

run: build
	./bin/async-job-queue

clean:
	rm -rf bin/

test:
	go test ./... -v