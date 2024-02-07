"""Module that manages Kafka Streaming service."""

import requests
import json
import time

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer


DEFAULT_ARGS = {
    "owner": "airscholar",
    "start_date": datetime(2024, 2, 1, 10,00 ,00),
}


def get_data() -> str:
    """Retrieve data from artificial user with each call"""

    response = requests.get("https://randomuser.me/api")
    
    if response.status_code == 200:
        response_content = response.json().get("results", None)
    else:
        raise Exception(f"Status code {response.status_code}: {response._content}")

    if response_content is not None:
        return response_content[0]
    else:
        raise Exception("Data could not be retrieved from API.")


def format_data(raw_user_data: dict) -> dict:
    """Prepare the response for Kafka streaming"""
    
    user_data = {}

    try:
        user_data["first_name"] = raw_user_data["name"]["first"]
        user_data["last_name"] = raw_user_data["name"]["last"]
        user_data["gender"] = raw_user_data["gender"]
        user_data["email"] = raw_user_data["email"]
        user_data["username"] = raw_user_data["login"]["username"]
        user_data["dob"] = raw_user_data["dob"]["date"]
        user_data["registered_date"] = raw_user_data["registered"]["date"]
        user_data["phone"] = raw_user_data["phone"]
        user_data["picture"] = raw_user_data["picture"]["medium"]
        user_data["postcode"] = raw_user_data["location"]["postcode"]
        user_data["address"] = \
            f'{raw_user_data["location"]["street"]["number"]} '\
            f'{raw_user_data["location"]["street"]["name"]} '\
            f'{raw_user_data["location"]["city"]}, '\
            f'{raw_user_data["location"]["state"]}, '\
            f'{raw_user_data["location"]["country"]}'
        
    except KeyError as key:
        raise KeyError(f"User data missing values: {key}")

    return user_data

    
def stream_data() -> None:
    """Get data from API, format it and stream it to Kafka"""

    api_response = get_data()
    format_response = format_data(raw_user_data=api_response)

    producer = KafkaProducer(bootstrap_servers=["localhost:9092"], max_block_ms=5000)
    producer.send("users_created", json.dumps(format_response).encode("utf-8"))


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

