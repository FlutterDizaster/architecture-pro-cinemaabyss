FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o events-service .

FROM alpine:3.20
COPY --from=builder /app/events-service /usr/local/bin/events-service
EXPOSE 8082
CMD ["events-service"]
