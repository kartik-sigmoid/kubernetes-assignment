from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
import psycopg2

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 3, 22),
    "retries": 0,
}

def create_table_and_populate_data():
    try:
        conn = psycopg2.connect(host="postgres-service-db", database="airflow", user="airflow", password="airflow", port='5432')
        cursor = conn.cursor()
        drop_table = """drop table IF EXISTS execution_table"""
        cursor.execute(drop_table)
        conn.commit()
        create_query = """create table execution_table as select dag_id, execution_date from dag_run order by execution_date;"""
        cursor.execute(create_query)
        conn.commit()
    except Exception as e:
        print(e)
    finally:
        conn.close()


with DAG("execution", default_args=default_args, schedule_interval="0 6 * * *", catchup = False) as dag:

    t1 = DummyOperator(task_id = "verify")

    t2 = PythonOperator(task_id = "create_and_fill_table", python_callable = create_table_and_populate_data)
    
    t1 >> t2
