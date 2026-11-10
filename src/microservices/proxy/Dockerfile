FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o proxy-service .

FROM alpine:3.20
COPY --from=builder /app/proxy-service /usr/local/bin/proxy-service
EXPOSE 8000
CMD ["proxy-service"]
