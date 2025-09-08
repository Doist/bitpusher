FROM --platform=$BUILDPLATFORM public.ecr.aws/docker/library/golang:alpine AS builder
WORKDIR /app
ENV CGO_ENABLED=0
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETARCH
RUN GOARCH=$TARGETARCH go build

FROM scratch
COPY --from=builder /app/bitpusher /
# workaround for Fargate 1.4.0 issue (Case ID 7085446141)
WORKDIR /etc
