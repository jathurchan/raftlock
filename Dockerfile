FROM golang:1.23.4 AS builder

WORKDIR /app

COPY . .

# Build statically-linked binary (no C dependencies) optimized for Linux/amd64 containers
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o raftlock cmd/main.go

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

COPY --from=builder /app/raftlock .

RUN chmod +x /root/raftlock

EXPOSE 50051

CMD ["./raftlock"]