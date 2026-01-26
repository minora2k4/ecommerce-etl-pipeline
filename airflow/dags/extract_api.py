from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import json
import time
import requests
from kafka import KafkaProducer

KAFKA_TOPIC = "ecommerce_products"
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"

def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5
    )

def crawl_ebay():
    headers = {
        "Authorization": f"Bearer {os.getenv('EBAY_TOKEN')}"
    }
    params = {
        "q": "gaming mouse",
        "limit": 1
    }
    response = requests.get(
        "https://api.ebay.com/buy/browse/v1/item_summary/search",
        headers=headers,
        params=params,
        timeout=10
    )
    response.raise_for_status()
    item = response.json()["itemSummaries"][0]

    return {
        "source": "ebay",
        "product_id": item["itemId"],
        "name": item["title"],
        "price": float(item["price"]["value"]),
        "currency": item["price"]["currency"],
        "timestamp": time.time()
    }

def crawl_aliexpress():
    headers = {
        "X-RapidAPI-Key": os.getenv("RAPIDAPI_KEY"),
        "X-RapidAPI-Host": "aliexpress-product-search.p.rapidapi.com"
    }
    params = {
        "query": "wireless earbuds",
        "page": 1
    }
    response = requests.get(
        "https://aliexpress-product-search.p.rapidapi.com/search",
        headers=headers,
        params=params,
        timeout=10
    )
    response.raise_for_status()
    item = response.json()["data"][0]

    return {
        "source": "aliexpress",
        "product_id": item["product_id"],
        "name": item["product_title"],
        "price": float(item["app_sale_price"]),
        "currency": "USD",
        "timestamp": time.time()
    }

def extract_and_send():
    producer = create_kafka_producer()

    records = [
        crawl_ebay(),
        crawl_aliexpress()
    ]
    for record in records:
        producer.send(KAFKA_TOPIC, value=record)

    producer.flush()
    producer.close()

default_args = {
    "owner": "airflow",
    "retries": 1,
    "start_date": datetime(2024, 1, 1)
}

with DAG(
    dag_id="extract_ecommerce_api_to_kafka",
    default_args=default_args,
    schedule_interval="@hourly",
    catchup=False,
    tags=["api", "kafka", "ecommerce"]
):
    extract_task = PythonOperator(
        task_id="extract_and_send_to_kafka",
        python_callable=extract_and_send
    )
