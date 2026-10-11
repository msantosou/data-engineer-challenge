FROM python:3.12-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -e ".[dev]"

# Install DuckDB CLI prerequisites
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates unzip && \
    rm -rf /var/lib/apt/lists/*

# Install DuckDB CLI
RUN curl -fsSL https://install.duckdb.org | sh

# Make the DuckDB CLI available globally
ENV PATH="/root/.duckdb/cli/latest:${PATH}"

# Verify installation
RUN duckdb --version

RUN duckdb -c "INSTALL ui;"