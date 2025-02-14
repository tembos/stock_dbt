# Use an official Python image
FROM python:3.9-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    DBT_PROFILES_DIR=/scotia_project_dbt
#    GOOGLE_APPLICATION_CREDENTIALS=dbt_service_account.json

# Set working directory in the container
WORKDIR /scotia_project_dbt

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install dbt for BigQuery
RUN pip install dbt-bigquery

# Copy dbt project files into the container
COPY . /scotia_project_dbt

# Ensure the entrypoint runs dbt commands
#ENTRYPOINT ["dbt"]
