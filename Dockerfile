# Build stage
FROM --platform=$BUILDPLATFORM golang:1.25.11-alpine AS builder

WORKDIR /app

# Install git and ca-certificates
RUN apk add --no-cache git ca-certificates

# Copy go.mod and go.sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the source code
COPY . .

# Build the Go application with cross-compilation
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /app/proxy ./cmd/proxy

# Run stage
FROM alpine:3.20

# Add ca-certificates for outgoing HTTPS requests
RUN apk add --no-cache ca-certificates

WORKDIR /app

# Copy the pre-built binary
COPY --from=builder /app/proxy /app/proxy

# Expose port
EXPOSE 8080

# Run the binary
CMD ["/app/proxy"]
