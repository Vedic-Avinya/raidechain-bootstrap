FROM golang:1.24-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o bootstrap ./cmd/main.go

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /app/bootstrap /usr/local/bin/bootstrap
EXPOSE 4001/udp 4001/tcp
CMD ["bootstrap"]
