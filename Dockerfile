# syntax=docker/dockerfile:1.7
FROM golang:1.26-alpine@sha256:30b1e546a9c7fa3b747c2bdba3a0c7bdf3a02ebb4c5b8405d2f948d7e8aac4ec AS builder

ARG BUILD_VERSION=dev
ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64 \
    GOPRIVATE=github.com/BCBP-SOLUTIONS-FZC-LLC \
    GONOSUMDB=github.com/BCBP-SOLUTIONS-FZC-LLC/*

WORKDIR /build

RUN apk add --no-cache git ca-certificates

COPY go.mod go.sum* ./
RUN --mount=type=secret,id=go_private_token \
    trap 'rm -f ~/.git-credentials ~/.gitconfig' EXIT && \
    git config --global credential.helper store && \
    echo "https://x-access-token:$(cat /run/secrets/go_private_token)@github.com" > ~/.git-credentials && \
    chmod 600 ~/.git-credentials && \
    go mod download && go mod verify

COPY . .

RUN go build \
    -ldflags="-w -s -X main.version=${BUILD_VERSION}" \
    -trimpath \
    -o /build/bin/workflow-stream-gateway \
    ./cmd/server

# gcr.io/distroless/static-debian12: no shell, no libc, no package manager.
# Runs as nonroot (uid 65532) by default.
FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab

COPY --from=builder /build/bin/workflow-stream-gateway /workflow-stream-gateway

EXPOSE 8080

ENTRYPOINT ["/workflow-stream-gateway"]
