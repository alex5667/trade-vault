# Python OBI Service Dockerfile
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Create non-root user
RUN useradd -m -u 1000 scanner && chown -R scanner:scanner /app

# Copy requirements and install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy service code
COPY book_obi_service.py ./

# Change ownership
RUN chown -R scanner:scanner /app

# Switch to non-root user
USER scanner

# Expose port
EXPOSE 8088

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8088/healthz || exit 1

# Run the service
CMD ["python", "book_obi_service.py"]

