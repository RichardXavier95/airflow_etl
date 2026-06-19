from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

MYSQL_CONN_ID = "mysql_source"
POSTGRES_CONN_ID = "postgres_target"

MYSQL_SCHEMA = "airflow_test"
MYSQL_TABLE = "employees"

POSTGRES_SCHEMA = "public"
POSTGRES_TABLE = "employees"

BATCH_SIZE = 1000


def transfer_data():
    mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    mysql_conn = mysql_hook.get_conn()
    mysql_cur = mysql_conn.cursor()

    pg_conn = pg_hook.get_conn()
    pg_cur = pg_conn.cursor()

    # Extract from MySQL
    mysql_cur.execute(f"SELECT * FROM `{MYSQL_SCHEMA}`.`{MYSQL_TABLE}`")

    columns = [col[0] for col in mysql_cur.description]

    quoted_cols = ", ".join([f'"{c}"' for c in columns])
    placeholders = ", ".join(["%s"] * len(columns))

    insert_sql = f"""
        INSERT INTO "{POSTGRES_SCHEMA}"."{POSTGRES_TABLE}"
        ({quoted_cols})
        VALUES ({placeholders})
    """

    while True:
        rows = mysql_cur.fetchmany(BATCH_SIZE)
        if not rows:
            break

        pg_cur.executemany(insert_sql, rows)
        pg_conn.commit()

    mysql_cur.close()
    mysql_conn.close()

    pg_cur.close()
    pg_conn.close()


with DAG(
    dag_id="mysql_to_postgres",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    transfer = PythonOperator(
        task_id="transfer_data",
        python_callable=transfer_data,
    )