FROM golang:1.26-bookworm

# Install basic dependencies and DuckDB build requirements
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    ca-certificates \
    gcc \
    g++ \
    cmake \
    libstdc++6 \
    make \
    && rm -rf /var/lib/apt/lists/*

# Install Go tools
RUN go install github.com/swaggo/swag/cmd/swag@v1.16.6 \
    && go install github.com/air-verse/air@latest \
    && go install golang.org/x/tools/gopls@latest \
    && go install github.com/go-delve/delve/cmd/dlv@latest

WORKDIR /workspace
