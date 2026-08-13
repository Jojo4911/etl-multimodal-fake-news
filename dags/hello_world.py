from airflow.sdk import DAG
from datetime import datetime
from airflow.providers.standard.operators.python import PythonOperator
import logging

log = logging.getLogger(__name__)

with DAG(
    "hello_world",
    schedule=None,
    start_date=datetime(2026, 8, 13),
    catchup=False,
) as dag:

    def hello():
        print("hello")
        return "First task: Whatever you return gets printed in the logs"

    first_step = PythonOperator(task_id="first_task", python_callable=hello)

    def world():
        print("world")
        return "Second task: Whatever you return gets printed in the logs"

    second_step = PythonOperator(task_id="second_task", python_callable=world)

    first_step >> second_step