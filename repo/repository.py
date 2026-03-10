from dagster import job, op


@op
def hello():
    print("Hello Dagster!")


@job
def hello_job():
    hello()
