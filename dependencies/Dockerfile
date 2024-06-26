# Stage 1: Base docker image with Airflow, Python, and MLflow dependencies
FROM apache/airflow:2.4.1-python3.10.9

# Set working directory
WORKDIR /app

# Install additional dependencies for RAG application, Airflow, and MLflow
COPY ./dependencies/requirements.txt /requirements.txt
RUN pip install --user --upgrade pip && \
    pip install -r /requirements.txt && \
    pip install mlflow

# Copy the entire application into the Docker image
COPY . /app

# Expose Airflow webserver port (if using for UI access)
EXPOSE 8080

# Start Airflow webserver and scheduler
CMD ["airflow", "webserver", "--port", "8080"]


# Stage 2: Docker image for Flask application deployment on Heroku

# Base docker image for Python and Flask dependencies
FROM python:3.10.9-slim

# Set working directory for Flask application
WORKDIR /app

# Copy and install requirements for Flask application
COPY ./flask_app/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire Flask application into the Docker image
COPY ./flask_app /app

# Expose Flask port (Heroku will handle port binding dynamically)
EXPOSE 5000

# Define command to run the Flask application
CMD ["gunicorn", "app:app", "--workers=4", "--bind", "0.0.0.0:5000"]