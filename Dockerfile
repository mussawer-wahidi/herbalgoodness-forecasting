# Use a lightweight Python base image
FROM python:3.10-slim

# Set working directory inside the container
WORKDIR /app

# Install system dependencies that might be needed
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Copy only requirements first to leverage Docker cache
COPY requirements.txt ./

# Install Python dependencies with verbose output
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir --verbose -r requirements.txt

# Copy the rest of your application code
COPY . .

# Create a non-root user for security
RUN useradd --create-home --shell /bin/bash app && chown -R app:app /app
USER app

# Set environment variables for Streamlit
ENV STREAMLIT_SERVER_HEADLESS=true
ENV STREAMLIT_SERVER_ENABLE_CORS=false
ENV STREAMLIT_SERVER_ENABLE_XSRF_PROTECTION=false
ENV STREAMLIT_BROWSER_GATHER_USAGE_STATS=false
ENV STREAMLIT_SERVER_FILE_WATCHER_TYPE=none
ENV STREAMLIT_SERVER_THEME_BASE=light

# Set Python environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Default port for Cloud Run
ENV PORT=8080

# Expose port
EXPOSE $PORT

# Create a startup script for better debugging
RUN echo '#!/bin/bash\n\
echo "=== Container Starting ==="\n\
echo "Python version: $(python --version)"\n\
echo "Current user: $(whoami)"\n\
echo "Working directory: $(pwd)"\n\
echo "Files in /app: $(ls -la /app)"\n\
echo "Environment variables:"\n\
env | grep -E "(PORT|STREAMLIT)"\n\
echo "=== Starting Streamlit ==="\n\
exec streamlit run Updated_Template.py \\\n\
  --server.address=0.0.0.0 \\\n\
  --server.port=$PORT \\\n\
  --server.headless=true \\\n\
  --server.enableCORS=false \\\n\
  --server.enableXsrfProtection=false \\\n\
  --server.fileWatcherType=none \\\n\
  --logger.level=debug\n' > /app/start.sh && chmod +x /app/start.sh

# Use the startup script
CMD ["/app/start.sh"]
