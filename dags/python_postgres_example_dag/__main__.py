from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import psycopg2.extras
import psycopg2
import pandas as pd
from airflow.hooks.base import BaseHook

CONNECTION_ID = "LOCAL_POSTGRES"

# people table creation query
# CREATE TABLE people(id SERIAL PRIMARY KEY, name VARCHAR(50), age INTEGER)

def read_table(postgres_config):
    print("Reading Table")
    
    pg_connection = psycopg2.connect(**postgres_config)
    query = 'SELECT * FROM people'

    # first we have to fetch the necessary information from the postgres universe table.
    with pg_connection.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as pg_cur:
        pg_cur.execute(query)
        df = pd.DataFrame(pg_cur.fetchall())

    pg_connection.close()
    
    print(df)

def add_rows(postgres_config):
    print("Adding rows into table.")
    df = pd.DataFrame({
        'name': ['Camille', 'Alan', 'Brad'],
        'age': [10, 10, 10],
    })
    data = list(map(tuple, df[['name', 'age']].values))
    print(data)
    pg_connection = psycopg2.connect(**postgres_config)
    
    query = "insert into people(name, age) values %s"
    with pg_connection.cursor() as pg_cur:
        psycopg2.extras.execute_values(pg_cur, query, data, template=None, page_size=100)

    pg_connection.commit()
    pg_connection.close()

def update_specific_rows(postgres_config):
    df = pd.DataFrame({
        'name': ['Camille', 'Alan'],
        'age': [40, 22],
    })
    update_query = """
    UPDATE people
    SET age = t.age
    FROM (VALUES %s) AS t(name, age)
    WHERE people.name = t.name;
    """
    
    pg_connection = psycopg2.connect(**postgres_config)
    with pg_connection.cursor() as pg_cur:
        psycopg2.extras.execute_values (
            pg_cur, update_query, list(map(tuple, df[['name', 'age']].values)), template=None, page_size=100
        )
    
    pg_connection.commit()
    pg_connection.close()


postgres = BaseHook.get_connection(CONNECTION_ID)

postgres_config = {
    "host":postgres.host,
    "port": postgres.port,
    "user": postgres.login,
    "password": postgres.password,
    "database": postgres.schema,
}

# Define your DAG
with DAG(
    'my_python_operator_dag',
    start_date=datetime(2023, 10, 11),
    schedule_interval=None,
    catchup=False,
) as dag:

    add_rows_task = PythonOperator(
        task_id='add_rows',  # Unique task ID
        python_callable=add_rows,
        op_kwargs={'postgres_config':postgres_config}
    )


    read_table_task = PythonOperator(
        task_id='read_table',  # Unique task ID
        python_callable=read_table,
        op_kwargs={'postgres_config':postgres_config}
    )

    update_specific_rows_task = PythonOperator(
        task_id='update_specific_rows',  # Unique task ID
        python_callable=update_specific_rows,
        op_kwargs={'postgres_config':postgres_config}
    )

# add_rows_task >> read_table_task >> update_specific_rows_task
