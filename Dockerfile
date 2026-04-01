FROM python:3.10-slim

WORKDIR /opt/dagster/app

RUN apt-get update \
	&& apt-get install -y --no-install-recommends openjdk-21-jre-headless git \
	&& rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Instalar paquete etl-jobs desde repositorio GitHub
ARG ETL_JOBS_GIT_URL=https://github.com/JEEF2601/etl-jobs.git
ARG ETL_JOBS_VERSION=v0.1.0
RUN pip install --no-cache-dir git+${ETL_JOBS_GIT_URL}@${ETL_JOBS_VERSION}

COPY repo ./repo
COPY job_registry.yaml ./job_registry.yaml
COPY dagster.yaml ./dagster.yaml

# Ensure Dagster has a persistent home path inside the container.
RUN mkdir -p /opt/dagster/dagster_home

ENV DAGSTER_HOME=/opt/dagster/dagster_home
ENV PYTHONPATH=/opt/dagster/app
