FROM golang:alpine AS builder
WORKDIR /app
ENV GOPROXY=https://proxy.golang.org CGO_ENABLED=0
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -ldflags='-s -w'

FROM scratch
COPY --from=builder /app/bitpusher /
# workaround for Fargate 1.4.0 issue (Case ID 7085446141)
WORKDIR /etc
