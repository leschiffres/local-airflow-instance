from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator


CONNECTION_ID = "LOCAL_POSTGRES"

query_truncate_table  = "TRUNCATE people"

query_add_rows = """
insert into people(name, age) values ('Alice', 24)
"""

query_update_age = """
UPDATE people SET age = age + 1
"""

with DAG(
    'baseline_postgres_operator_dag',
    start_date=datetime(2023, 10, 11),
    schedule_interval=None,
    catchup=False,
) as dag:

    truncate_table_task = PostgresOperator(
        task_id='truncate_table',  # Unique task ID
        postgres_conn_id=CONNECTION_ID,
        sql=query_truncate_table,
    )

    add_rows_task = PostgresOperator(
        task_id='add_rows',  # Unique task ID
        postgres_conn_id=CONNECTION_ID,
        sql=query_add_rows,
    )
    
    update_age_task = PostgresOperator(
        task_id='update_age',  # Unique task ID
        postgres_conn_id=CONNECTION_ID,
        sql=query_update_age,
    )

truncate_table_task >> add_rows_task >> update_age_task
