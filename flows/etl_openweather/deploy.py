from prefect.runner.storage import LocalStorage
from banco import insert_dados

if __name__ == "__main__":
    insert_dados.from_source(
        source=LocalStorage("/flows"),
        entrypoint="etl_openweather/banco.py:insert_dados",
    ).deploy(
        name="ETL Openweather",
        work_pool_name="Work Pool",
        cron="35 * * * *",
    )