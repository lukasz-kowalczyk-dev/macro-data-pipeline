# =============================================================================
# Dockerfile — macro-data-pipeline
#
# Build image:
#   docker build -t macro-data-pipeline .
#
# Local run (dry-run, no BigQuery):
#   docker run --rm macro-data-pipeline python -m pipeline.main --dry-run
#
# Run HTTP server (like Cloud Run):
#   docker run --rm -p 8080:8080 \
#     -e GCP_PROJECT_ID=your-project \
#     -e BQ_DATASET=macro_data \
#     -e GOOGLE_APPLICATION_CREDENTIALS=/secrets/sa.json \
#     -v /path/to/sa.json:/secrets/sa.json \
#     macro-data-pipeline
# =============================================================================

# --- Stage 1: Builder ---
# Install dependencies in a separate stage to keep the production image smaller
FROM python:3.11-slim AS builder

WORKDIR /build

# Copy only the files needed to install dependencies
COPY pyproject.toml .
COPY src/ ./src/

# Install in editable mode into a temporary directory
RUN pip install --no-cache-dir --prefix=/install -e .


# --- Stage 2: Production image ---
FROM python:3.11-slim

# Image metadata
LABEL org.opencontainers.image.source="https://github.com/lukasz-kowalczyk-dev/macro-data-pipeline"
LABEL org.opencontainers.image.description="ETL pipeline: OECD & IMF → BigQuery"

# Do not run as root — security best practice
RUN useradd --create-home --shell /bin/bash pipeline
USER pipeline
WORKDIR /app

# Copy installed packages from builder stage
COPY --from=builder /install /usr/local
# Copy source code
COPY --from=builder /build/src ./src

# Cloud Run requires listening on this port
ENV PORT=8080
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Health check — Cloud Run checks whether the container is alive
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health')"

# Start Flask server — Cloud Run will send an HTTP request to trigger the pipeline
CMD ["python", "-m", "pipeline.main", "--serve", "--port", "8080"]
