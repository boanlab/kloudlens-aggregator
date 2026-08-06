# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 BoanLab @ DKU
#
# kloudlens-aggregator — Makefile.
#
# The kloudlens module is a normal Go-module dependency
# (`github.com/boanlab/kloudlens` v0.1.x), fetched via the module proxy.
# No sibling checkout required.
#
# Targets:
#   build         — static binary into bin/kloudlens-aggregator
#   cross         — build for a specific GOOS/GOARCH into OUT
#   install       — install bin/kloudlens-aggregator into $(GOBIN)
#                   (or $HOME/go/bin)
#   build-image   — docker build boanlab/kloudlens-aggregator:$(TAG) + :latest
#   push-image    — docker push boanlab/kloudlens-aggregator:$(TAG) + :latest
#   gofmt         — gofmt -s -w ./...
#   vet           — go vet ./...
#   golangci-lint — golangci-lint run ./...
#   gosec         — gosec scan
#   test          — go test -race -count=1 ./...
#   verify        — gofmt + vet + golangci-lint + gosec + test (CI gate)
#   clean         — remove bin/

PROG_NAME  = kloudlens-aggregator
IMAGE_NAME = boanlab/kloudlens-aggregator
TAG        ?= $(shell git describe --tags --dirty --always 2>/dev/null || echo dev)

LDFLAGS = -s -w -X main.version=$(TAG)

GO_FILES = $(shell find . -type f -name '*.go' -not -path './vendor/*')

.PHONY: all build cross build-image push-image gofmt vet golangci-lint gosec \
        install verify test clean run

all: build

## build: vet + format check + build the binary
build: gofmt vet
	@mkdir -p bin
	CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o bin/$(PROG_NAME) ./cmd/$(PROG_NAME)

## cross: build for a specific GOOS/GOARCH into OUT
##   make cross GOOS=linux GOARCH=arm64 OUT=kloudlens-aggregator-linux-arm64
OUT ?= bin/$(PROG_NAME)
cross:
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) go build -trimpath -ldflags '$(LDFLAGS)' -o $(OUT) ./cmd/$(PROG_NAME)

## test: run the full unit-test suite
test:
	go test ./... -v -count=1 -race

## gofmt: rewrite files to canonical gofmt output
gofmt:
	gofmt -s -w $(GO_FILES)

## vet: run `go vet`
vet:
	go vet ./...

## golangci-lint: run golangci-lint (installs it on demand)
golangci-lint:
ifeq (, $(shell which golangci-lint))
	@{ \
	set -e ;\
	tmp=$$(mktemp -d) ;\
	cd $$tmp ;\
	go mod init tmp ;\
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest ;\
	rm -rf $$tmp ;\
	}
endif
	golangci-lint run

## gosec: run gosec static analysis (installs it on demand)
gosec:
ifeq (, $(shell which gosec))
	@{ \
	set -e ;\
	tmp=$$(mktemp -d) ;\
	cd $$tmp ;\
	go mod init tmp ;\
	go install github.com/securego/gosec/v2/cmd/gosec@latest ;\
	rm -rf $$tmp ;\
	}
endif
	gosec -quiet -exclude-generated ./...

## install: install bin/kloudlens-aggregator into $(GOBIN) (or $HOME/go/bin)
install: build
	install -D -m 0755 bin/$(PROG_NAME) $${GOBIN:-$$HOME/go/bin}/$(PROG_NAME)

## verify: gate that CI runs — fmt + vet + lint + sec + test
verify: gofmt vet golangci-lint gosec test

## run: build and print --help
run: build
	./bin/$(PROG_NAME) --help

clean:
	rm -rf bin

build-image:
	docker build --build-arg VERSION=$(TAG) -f Dockerfile -t $(IMAGE_NAME):$(TAG) -t $(IMAGE_NAME):latest .

push-image: build-image
	docker push $(IMAGE_NAME):$(TAG)
ifneq ($(TAG),latest)
	docker push $(IMAGE_NAME):latest
endif
