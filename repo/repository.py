from dagster import Config, Definitions, RetryPolicy, ScheduleDefinition, job, op

try:
    from .spark_runner import run_spark_job
except ImportError:  # pragma: no cover - fallback for direct module execution
    from spark_runner import run_spark_job


@op
def hello() -> None:
    print("Hello Dagster!")


@op(retry_policy=RetryPolicy(max_retries=3, delay=30))
def run_influx_cobre_etl() -> None:
    run_spark_job("influx_cobre")


@op(retry_policy=RetryPolicy(max_retries=3, delay=30))
def run_cryptocompare_spark_etl() -> None:
    run_spark_job("cryptocompare_to_r2")


@job
def hello_job() -> None:
    hello()


@job
def influx_cobre_etl_job() -> None:
    run_influx_cobre_etl()


@job
def cryptocompare_r2_etl_job() -> None:
    run_cryptocompare_spark_etl()


# ---------------------------------------------------------------------------
# Silver: Energía eléctrica desde InfluxDB
# ---------------------------------------------------------------------------

class SilverInfluxElectricityConfig(Config):
    start_date: str = ""
    end_date: str = ""


@op(retry_policy=RetryPolicy(max_retries=3, delay=30))
def run_silver_influx_electricity_etl(config: SilverInfluxElectricityConfig) -> None:
    params: dict[str, str] = {}
    if config.start_date.strip():
        params["start_date"] = config.start_date.strip()
    if config.end_date.strip():
        params["end_date"] = config.end_date.strip()
    run_spark_job("silver_influx_electricity", params or None)


@job
def silver_influx_electricity_etl_job() -> None:
    run_silver_influx_electricity_etl()


hourly_influx_cobre_schedule = ScheduleDefinition(
    job=influx_cobre_etl_job,
    cron_schedule="0 * * * *",
    execution_timezone="UTC",
)


daily_cryptocompare_r2_schedule = ScheduleDefinition(
    job=cryptocompare_r2_etl_job,
    cron_schedule="30 0 * * *",
    execution_timezone="UTC",
)

# Cada lunes a las 02:00 UTC procesa la semana anterior completa (sin config = defaults)
weekly_silver_influx_electricity_schedule = ScheduleDefinition(
    job=silver_influx_electricity_etl_job,
    cron_schedule="0 2 * * 1",
    execution_timezone="UTC",
)


defs = Definitions(
    jobs=[hello_job, influx_cobre_etl_job, cryptocompare_r2_etl_job, silver_influx_electricity_etl_job],
    schedules=[hourly_influx_cobre_schedule, daily_cryptocompare_r2_schedule, weekly_silver_influx_electricity_schedule],
)
