FROM golang:1.24-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o processor ./cmd/processor

FROM alpine:3.19
WORKDIR /app
COPY --from=build /app/processor .
COPY config/config.yaml .
EXPOSE 9090 8081
CMD ["./processor"]