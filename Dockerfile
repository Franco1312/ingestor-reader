# Use standard Python base image for ECS/Fargate
FROM python:3.12-slim

WORKDIR /app

# Install system dependencies if needed (currently none, but ready for future needs)
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     <package-name> \
#     && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ /app/src/
COPY config/ /app/config/

# Copy entrypoint script
COPY docker-entrypoint.sh /app/
RUN chmod +x /app/docker-entrypoint.sh

# Set entrypoint
ENTRYPOINT ["/app/docker-entrypoint.sh"]

