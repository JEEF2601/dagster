import os
import subprocess
from pathlib import Path

from dagster import Definitions, RetryPolicy, ScheduleDefinition, job, op
from pyspark.sql import SparkSession
import runpy

@op
def run_spark_etl_remote(script_name: str) -> None:
    remote_url = os.getenv("SPARK_REMOTE_URL", "sc://spark-master:15002")
    os.environ["SPARK_REMOTE"] = remote_url
    
    project_root = Path(__file__).resolve().parents[1]
    spark_script = project_root / "spark_jobs" / script_name
    
    # Execute the script in the context of the Spark Connect session
    runpy.run_path(str(spark_script), run_name="__main__")

@op(retry_policy=RetryPolicy(max_retries=3, delay=30))
def run_spark_etl() -> None:
    run_spark_etl_remote("influx_to_r2.py")

@op(retry_policy=RetryPolicy(max_retries=3, delay=30))
def run_cryptocompare_spark_etl() -> None:
    run_spark_etl_remote("cryptocompare_to_r2.py")


@job
def influx_r2_etl_job() -> None:
    run_spark_etl()


@job
def cryptocompare_r2_etl_job() -> None:
    run_cryptocompare_spark_etl()


hourly_influx_r2_schedule = ScheduleDefinition(
    job=influx_r2_etl_job,
    cron_schedule="0 * * * *",
    execution_timezone="UTC",
)


daily_cryptocompare_r2_schedule = ScheduleDefinition(
    job=cryptocompare_r2_etl_job,
    cron_schedule="30 0 * * *",
    execution_timezone="UTC",
)


defs = Definitions(
    jobs=[influx_r2_etl_job, cryptocompare_r2_etl_job],
    schedules=[hourly_influx_r2_schedule, daily_cryptocompare_r2_schedule],
)
