import os
from typing import Any

import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType


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
    app_name = os.getenv("SPARK_APP_NAME", "cryptocompare_to_r2_etl")
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


def _fetch_cryptocompare_data() -> list[dict[str, Any]]:
    url = "https://min-api.cryptocompare.com/data/v2/histoday"
    params = {
        "fsym": "BTC",
        "tsym": "USD",
        "limit": "6",
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()

    if payload.get("Response") == "Error":
        message = payload.get("Message", "Unknown error from CryptoCompare")
        raise RuntimeError(f"CryptoCompare API error: {message}")

    data = payload.get("Data", {})
    rows = data.get("Data", []) if isinstance(data, dict) else []
    if not isinstance(rows, list):
        raise RuntimeError("Unexpected CryptoCompare response format.")

    return rows


def _schema() -> StructType:
    return StructType(
        [
            StructField("time", LongType(), True),
            StructField("close", DoubleType(), True),
            StructField("high", DoubleType(), True),
            StructField("low", DoubleType(), True),
            StructField("open", DoubleType(), True),
            StructField("volumefrom", DoubleType(), True),
            StructField("volumeto", DoubleType(), True),
            StructField("conversionType", StringType(), True),
            StructField("conversionSymbol", StringType(), True),
        ]
    )


def _safe_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized_rows: list[dict[str, Any]] = []
    numeric_fields = ("close", "high", "low", "open", "volumefrom", "volumeto")

    for row in rows:
        normalized_row: dict[str, Any] = {
            "time": _safe_int(row.get("time")),
            "conversionType": (
                str(row.get("conversionType"))
                if row.get("conversionType") is not None
                else None
            ),
            "conversionSymbol": (
                str(row.get("conversionSymbol"))
                if row.get("conversionSymbol") is not None
                else None
            ),
        }

        for field in numeric_fields:
            normalized_row[field] = _safe_float(row.get(field))

        normalized_rows.append(normalized_row)

    return normalized_rows


def _prepare_frames(spark: SparkSession, rows: list[dict[str, Any]]):
    normalized_rows = _normalize_rows(rows)
    if not normalized_rows:
        return None, None

    df_raw = spark.createDataFrame(normalized_rows, schema=_schema()).withColumn(
        "fecha", F.from_unixtime(F.col("time")).cast("date")
    )

    df_silver = (
        df_raw.select(
            F.col("fecha"),
            F.col("close").alias("precio_cierre"),
            F.col("high").alias("maximo"),
            F.col("low").alias("minimo"),
            F.col("volumefrom").alias("volumen_btc"),
        )
        .dropna(subset=["fecha"])
        .dropDuplicates(["fecha"])
    )

    return df_raw, df_silver


def _target_paths() -> tuple[str, str]:
    bucket = _get_env("R2_BUCKET")
    base_prefix = "btc-lakehouse-bucket"
    base_path = f"s3a://{bucket}/{base_prefix}/"
    return f"{base_path}bronze/", f"{base_path}silver/"


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


def _write_partitioned_parquet(spark: SparkSession, df, target_path: str, label: str) -> None:
    _cleanup_spark_staging_dirs(spark, target_path)
    print(f"Refreshing {label} data in {target_path} for the loaded date partitions.")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    (
        df.write.mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("fecha")
        .parquet(target_path)
    )
    _cleanup_spark_staging_dirs(spark, target_path)


def main() -> None:
    spark = None
    try:
        spark = _build_spark_session()
        rows = _fetch_cryptocompare_data()

        if not rows:
            print("No rows returned from CryptoCompare. Skipping write.")
            return

        df_raw, df_silver = _prepare_frames(spark, rows)
        if df_raw is None or df_silver is None:
            print("No rows available after normalization. Skipping write.")
            return

        bronze_path, silver_path = _target_paths()
        _write_partitioned_parquet(spark, df_raw, bronze_path, "bronze")
        _write_partitioned_parquet(spark, df_silver, silver_path, "silver")
        print("ETL completed successfully.")
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()