# StreamLineHub Backend Dockerfile

FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies in chunks to avoid timeout
RUN pip install --upgrade pip

# Install core dependencies first
RUN pip install --no-cache-dir --timeout=300 --retries=5 \
    fastapi==0.104.1 \
    uvicorn[standard]==0.24.0 \
    pydantic==2.5.2 \
    pydantic-settings==2.1.0

# Install database and cache dependencies
RUN pip install --no-cache-dir --timeout=300 --retries=5 \
    motor==3.3.2 \
    pymongo==4.6.1 \
    redis==5.0.1

# Install remaining dependencies
RUN pip install --no-cache-dir --timeout=300 --retries=5 -r requirements.txt

# Copy application code
COPY src/ ./src/
COPY models/ ./models/
COPY config/ ./config/

# Create necessary directories
RUN mkdir -p /app/logs /app/data

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Default command
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
