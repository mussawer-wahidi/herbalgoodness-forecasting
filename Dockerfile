# Use a lightweight Python base image
FROM python:3.10-slim

# Set working directory inside the container
WORKDIR /app

# Copy only requirements first to leverage Docker cache
COPY requirements.txt ./

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your application code
COPY . .

# Set environment variables for Streamlit
ENV STREAMLIT_SERVER_HEADLESS=true
ENV STREAMLIT_SERVER_PORT=8080
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0
ENV PORT=8080

# Expose port
EXPOSE 8080

# Add a simple health check script
RUN echo '#!/bin/bash
echo "Container starting..."
streamlit run Updated_Template.py \
 --server.address=0.0.0.0 \
 --server.port=8080 \
 --server.headless=true \
 --server.enableCORS=false \
 --server.enableXsrfProtection=false' > /app/start.sh && chmod +x /app/start.sh

# Run the app
CMD ["/bin/bash", "/app/start.sh"]
