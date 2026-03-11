import os
import subprocess
from pathlib import Path

from dagster import Definitions, RetryPolicy, ScheduleDefinition, job, op


@op
def hello() -> None:
    print("Hello Dagster!")


@op(retry_policy=RetryPolicy(max_retries=3, delay=30))
def run_spark_etl() -> None:
    project_root = Path(__file__).resolve().parents[1]
    spark_script = project_root / "spark_jobs" / "influx_to_r2.py"
    spark_packages = os.getenv("SPARK_PACKAGES", "org.apache.hadoop:hadoop-aws:3.4.2").strip()

    command = ["spark-submit"]
    if spark_packages:
        command.extend(["--packages", spark_packages])
    command.append(str(spark_script))

    subprocess.run(
        command,
        check=True,
    )


@job
def hello_job() -> None:
    hello()


@job
def influx_r2_etl_job() -> None:
    run_spark_etl()


hourly_influx_r2_schedule = ScheduleDefinition(
    job=influx_r2_etl_job,
    cron_schedule="0 * * * *",
    execution_timezone="UTC",
)


defs = Definitions(
    jobs=[hello_job, influx_r2_etl_job],
    schedules=[hourly_influx_r2_schedule],
)
