# syntax=docker/dockerfile:1.7
FROM golang:1.26-alpine@sha256:5cb67f19c5e7994dcb80c074f7cfca8aeeada45bbdca035f73df269cefdcce61 AS builder

ARG BUILD_VERSION=dev
ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64 \
    GOPRIVATE=github.com/BCBP-SOLUTIONS-FZC-LLC \
    GONOSUMDB=github.com/BCBP-SOLUTIONS-FZC-LLC/*

WORKDIR /build

RUN apk add --no-cache git ca-certificates

RUN --mount=type=secret,id=go_private_token \
    git config --global credential.helper store && \
    echo "https://x-access-token:$(cat /run/secrets/go_private_token)@github.com" > ~/.git-credentials && \
    chmod 600 ~/.git-credentials

COPY go.mod go.sum* ./
RUN go mod download && go mod verify

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
