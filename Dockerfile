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

# Expose port (informational only for Docker)
EXPOSE 8080

# Run the app - use Cloud Run's $PORT dynamically
CMD streamlit run Updated_Template.py --server.address=0.0.0.0 --server.port=${PORT}
