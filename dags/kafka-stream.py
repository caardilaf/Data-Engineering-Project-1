from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner": "airscholar",
    "start_date": datetime(2024, 2, 1, 10,00 ,00),
}

def stream_data():

    import json
    import requests

    res = requests.get("https://randomuser.me/api")
    
    return res.json()
    

with DAG(
    dag_id="user_automation",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    catchup=False,
    ) as dag:

    streaming_task = PythonOperator(
        python_callable=stream_data,
        task_id="stream_data_from_api",
    )


if __name__ == "__main__":

    stream_data()

