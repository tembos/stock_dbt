FROM python:3.9-slim

ENV PYTHONUNBUFFERED=1 \
    DBT_PROFILES_DIR=/scotia_project_dbt\
    GOOGLE_APPLICATION_CREDENTIALS=dbt_service_account.json

WORKDIR /scotia_project_dbt

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN pip install dbt-bigquery

COPY . /scotia_project_dbt

#ENTRYPOINT ["dbt"]
