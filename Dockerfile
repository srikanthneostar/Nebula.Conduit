FROM golang:1.21 as builder
WORKDIR /app
COPY . .
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o nebula-conduit ./cmd/server

FROM alpine:latest
RUN apk --no-cache add ca-certificates python3
WORKDIR /root/
COPY --from=builder /app/nebula-conduit .
COPY config/config.yaml .
RUN mkdir -p /opt/pyexec/scripts
EXPOSE 8080
CMD ["./nebula-conduit"]
