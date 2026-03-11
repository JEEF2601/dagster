FROM python:3.10-slim

WORKDIR /opt/dagster/app

RUN apt-get update \
	&& apt-get install -y --no-install-recommends openjdk-21-jre-headless \
	&& rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY repo ./repo
COPY spark_jobs ./spark_jobs

# Ensure Dagster has a persistent home path inside the container.
RUN mkdir -p /opt/dagster/dagster_home

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV DAGSTER_HOME=/opt/dagster/dagster_home
ENV PYTHONPATH=/opt/dagster/app
