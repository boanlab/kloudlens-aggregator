# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 BoanLab @ DKU
#
# kloudlens-aggregator image — cluster fan-in Deployment.
# Build context is the Go module root (where go.mod lives). The kloudlens
# module is a normal Go-module dependency fetched via the module proxy —
# no sibling checkout required, so no `git` is needed in the build image.
#
#   docker build -t boanlab/kloudlens-aggregator:<tag> .
#   # or via make:
#   make build-image TAG=<tag>

FROM golang:1.24-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/kloudlens-aggregator ./cmd/kloudlens-aggregator

FROM alpine:3.21

RUN apk add --no-cache ca-certificates tini

COPY --from=build /out/kloudlens-aggregator /usr/local/bin/kloudlens-aggregator

USER 65532:65532
ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/kloudlens-aggregator"]
CMD ["--help"]
