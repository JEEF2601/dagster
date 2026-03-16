import os
import re
from typing import Any

import pandas as pd
from influxdb import DataFrameClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def _get_env(name: str, default: str | None = None, required: bool = True) -> str:
    value = os.getenv(name, default)
    if required and (value is None or value.strip() == ""):
        raise ValueError(f"Missing required environment variable: {name}")
    return value  # type: ignore[return-value]


def _build_spark_session() -> SparkSession:
    access_key = _get_env("R2_ACCESS_KEY_ID")
    secret_key = _get_env("R2_SECRET_ACCESS_KEY")
    endpoint = _get_env("R2_ENDPOINT").rstrip("/")
    if "//" not in endpoint:
        endpoint = f"https://{endpoint}"
    ssl_enabled = "true" if endpoint.startswith("https://") else "false"
    region = os.getenv("R2_REGION", "auto")
    app_name = os.getenv("SPARK_APP_NAME", "influx_to_r2_etl")
    spark_packages = os.getenv("SPARK_PACKAGES", "").strip()

    builder = SparkSession.builder.appName(app_name)
    if spark_packages:
        builder = builder.config("spark.jars.packages", spark_packages)

    return (
        builder
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.endpoint.region", region)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", ssl_enabled)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def _default_influx_query() -> str:
    lookback = os.getenv("INFLUXDB_LOOKBACK", "30d").strip()
    if not re.fullmatch(r"\d+(ms|s|m|h|d|w)", lookback):
        raise ValueError(
            "Invalid INFLUXDB_LOOKBACK value. Use formats like '1h', '12h', '7d', or '30d'."
        )

    return f"SELECT * FROM /.*/ WHERE time > now() - {lookback}"


def _query_influx_as_pandas() -> pd.DataFrame:
    influx_host = _get_env("INFLUXDB_HOST")
    influx_port = int(_get_env("INFLUXDB_PORT", default="8086"))
    influx_database = _get_env("INFLUXDB_DATABASE")
    influx_username = _get_env("INFLUXDB_USERNAME")
    influx_password = _get_env("INFLUXDB_PASSWORD")

    influx_query = os.getenv("INFLUXDB_QUERY", "").strip() or _default_influx_query()

    client = DataFrameClient(
        host=influx_host,
        port=influx_port,
        username=influx_username,
        password=influx_password,
        database=influx_database,
    )
    try:
        result: Any = client.query(influx_query)
    finally:
        client.close()

    if not result:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    if hasattr(result, "items"):
        for key, frame in result.items():
            if frame is None or frame.empty:
                continue

            normalized = frame.reset_index()
            if "time" in normalized.columns:
                normalized = normalized.rename(columns={"time": "timestamp"})
            elif "index" in normalized.columns:
                normalized = normalized.rename(columns={"index": "timestamp"})

            measurement = key[0] if isinstance(key, tuple) else str(key)
            normalized["measurement"] = measurement
            frames.append(normalized)

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


def _prepare_spark_frame(spark: SparkSession, pandas_df: pd.DataFrame):
    if pandas_df.empty:
        return None

    spark_df = spark.createDataFrame(pandas_df)
    drop_candidates = ["result", "table", "_start", "_stop"]
    columns_to_drop = [column for column in drop_candidates if column in spark_df.columns]
    if columns_to_drop:
        spark_df = spark_df.drop(*columns_to_drop)

    if "_time" in spark_df.columns:
        spark_df = spark_df.withColumnRenamed("_time", "timestamp")
    if "time" in spark_df.columns and "timestamp" not in spark_df.columns:
        spark_df = spark_df.withColumnRenamed("time", "timestamp")

    if "timestamp" not in spark_df.columns:
        raise RuntimeError("Influx query result must include '_time' or 'timestamp'.")

    return (
        spark_df.withColumn("timestamp", F.to_timestamp("timestamp"))
        .withColumn("date", F.to_date("timestamp"))
    )


def _target_path() -> str:
    bucket = _get_env("R2_BUCKET")
    prefix = os.getenv("R2_PREFIX", "influx/data").strip("/")
    if prefix:
        return f"s3a://{bucket}/{prefix}/"
    return f"s3a://{bucket}/"


def _cleanup_spark_staging_dirs(spark: SparkSession, target_path: str) -> None:
    hadoop_conf = spark._jsc.hadoopConfiguration()
    path = spark._jvm.org.apache.hadoop.fs.Path(target_path)
    fs = path.getFileSystem(hadoop_conf)

    if not fs.exists(path):
        return

    for status in fs.listStatus(path):
        item_path = status.getPath()
        if item_path.getName().startswith(".spark-staging-"):
            fs.delete(item_path, True)
            print(f"Deleted Spark staging directory: {item_path.toString()}")


def main() -> None:
    spark = None
    try:
        spark = _build_spark_session()
        pandas_df = _query_influx_as_pandas()

        if pandas_df.empty:
            print("No rows returned from InfluxDB. Skipping write.")
            return

        df_clean = _prepare_spark_frame(spark, pandas_df)
        if df_clean is None:
            print("No rows available after normalization. Skipping write.")
            return

        target_path = _target_path()
        _cleanup_spark_staging_dirs(spark, target_path)
        print(f"Refreshing transformed data in {target_path} for the loaded date partitions.")
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        (
            df_clean.write.mode("overwrite")
            .option("partitionOverwriteMode", "dynamic")
            .partitionBy("date")
            .parquet(target_path)
        )
        _cleanup_spark_staging_dirs(spark, target_path)
        print("ETL completed successfully.")
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
