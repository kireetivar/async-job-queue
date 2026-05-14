.DEFAULT_GOAL= build

.PHONY: run fmt vet build clean test

fmt: 
	go fmt ./...

vet: fmt
	go vet ./...

build: vet
	go build -o bin/async-job-queue .

run: build
	./bin/async-job-queue

clean:
	rm -rf bin/

test:
	go test ./... -v