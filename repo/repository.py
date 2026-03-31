from dagster import Definitions, RetryPolicy, ScheduleDefinition, job, op

try:
    from .spark_runner import run_spark_job
except ImportError:  # pragma: no cover - fallback for direct module execution
    from spark_runner import run_spark_job


@op
def hello() -> None:
    print("Hello Dagster!")


@op(retry_policy=RetryPolicy(max_retries=3, delay=30))
def run_spark_etl() -> None:
    run_spark_job("influx_to_r2")


@op(retry_policy=RetryPolicy(max_retries=3, delay=30))
def run_cryptocompare_spark_etl() -> None:
    run_spark_job("cryptocompare_to_r2")


@job
def hello_job() -> None:
    hello()


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
    jobs=[hello_job, influx_r2_etl_job, cryptocompare_r2_etl_job],
    schedules=[hourly_influx_r2_schedule, daily_cryptocompare_r2_schedule],
)
