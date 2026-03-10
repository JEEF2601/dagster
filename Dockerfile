FROM python:3.10-slim

WORKDIR /opt/dagster/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY repo ./repo

# Ensure Dagster has a persistent home path inside the container.
RUN mkdir -p /opt/dagster/dagster_home

ENV DAGSTER_HOME=/opt/dagster/dagster_home
ENV PYTHONPATH=/opt/dagster/app
