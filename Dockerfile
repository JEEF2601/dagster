FROM python:3.10-slim

WORKDIR /opt/dagster/app

RUN apt-get update \
	&& apt-get install -y --no-install-recommends ca-certificates openjdk-21-jre-headless git wget \
	&& rm -rf /var/lib/apt/lists/*

RUN set -eux; \
	mkdir -p /opt/spark/local-jars; \
	wget -nv -O /opt/spark/local-jars/hadoop-aws-3.4.1.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar; \
	wget -nv -O /opt/spark/local-jars/bundle-2.29.52.jar https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar; \
	wget -nv -O /opt/spark/local-jars/analyticsaccelerator-s3-1.2.1.jar https://repo1.maven.org/maven2/software/amazon/s3/analyticsaccelerator/analyticsaccelerator-s3/1.2.1/analyticsaccelerator-s3-1.2.1.jar; \
	wget -nv -O /opt/spark/local-jars/wildfly-openssl-2.1.4.Final.jar https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/2.1.4.Final/wildfly-openssl-2.1.4.Final.jar

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Instalar paquete etl-jobs desde repositorio GitHub
ARG ETL_JOBS_GIT_URL=https://github.com/JEEF2601/etl-jobs.git
ARG ETL_JOBS_VERSION=v0.1.4
RUN pip install --no-cache-dir git+${ETL_JOBS_GIT_URL}@${ETL_JOBS_VERSION}

COPY repo ./repo
COPY job_registry.yaml ./job_registry.yaml
COPY dagster.yaml ./dagster.yaml

# Ensure Dagster has a persistent home path inside the container.
RUN mkdir -p /opt/dagster/dagster_home

ENV DAGSTER_HOME=/opt/dagster/dagster_home
ENV PYTHONPATH=/opt/dagster/app
