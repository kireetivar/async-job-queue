.DEFAULT_GOAL= build

.PHONY: run fmt vet build

fmt: 
	go fmt ./...
vet:
	go vet ./...
build: fmt
	go build