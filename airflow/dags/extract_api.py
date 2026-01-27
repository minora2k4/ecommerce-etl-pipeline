from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import time
import requests
from kafka import KafkaProducer
from concurrent.futures import ThreadPoolExecutor, as_completed

KAFKA_TOPIC = "ecommerce_products"
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"


def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5
    )


def crawl_dummyjson():
    """
    DummyJSON - Free fake REST API
    Không cần đăng ký, authentication, hoặc API key
    Có 194 sản phẩm với đầy đủ thông tin
    """
    skip = int(time.time()) % 144  # Random offset để có data đa dạng hơn
    response = requests.get(
        "https://dummyjson.com/products",
        params={
            "limit": 50,  # Tăng lên 50 items
            "skip": skip,
            "select": "title,price,category,brand,rating,stock,thumbnail"
        },
        timeout=10
    )
    response.raise_for_status()
    data = response.json()

    records = []
    for item in data["products"]:
        records.append({
            "source": "dummyjson",
            "product_id": item["id"],
            "name": item["title"],
            "price": float(item["price"]),
            "currency": "USD",
            "category": item.get("category", "unknown"),
            "brand": item.get("brand", "unknown"),
            "rating": item.get("rating", 0),
            "stock": item.get("stock", 0),
            "thumbnail": item.get("thumbnail", ""),
            "timestamp": time.time()
        })

    return records


def crawl_fakestoreapi():
    """
    FakeStoreAPI - Free fake REST API for e-commerce
    Không cần đăng ký hoặc API key
    Có 20 sản phẩm với đầy đủ thông tin
    Strategy: Lấy tất cả products và duplicate với variation để đạt 50 records
    """
    response = requests.get(
        "https://fakestoreapi.com/products",
        timeout=10
    )
    response.raise_for_status()
    products = response.json()

    records = []

    # Lặp lại products để đạt 50 records với slight variations
    for round_num in range(3):  # 3 rounds = 20x3 = 60 products
        for item in products:
            if len(records) >= 50:  # Giới hạn 50 records
                break

            records.append({
                "source": "fakestoreapi",
                "product_id": f"{item['id']}_r{round_num}",  # Unique ID mỗi round
                "name": item["title"],
                "price": float(item["price"]) * (1 + round_num * 0.05),  # Price variation
                "currency": "USD",
                "category": item.get("category", "unknown"),
                "rating": item.get("rating", {}).get("rate", 0),
                "rating_count": item.get("rating", {}).get("count", 0),
                "description": item.get("description", ""),
                "image": item.get("image", ""),
                "timestamp": time.time()
            })

        if len(records) >= 50:
            break

    return records[:50]  # Đảm bảo đúng 50 records


def crawl_platzi_fakestore():
    """
    Platzi Fake Store API - Free fake REST API
    Không cần đăng ký hoặc API key
    Hỗ trợ filtering và có nhiều sản phẩm
    Strategy: Lấy nhiều products và filter để đạt đúng 50 valid records
    """
    response = requests.get(
        "https://api.escuelajs.co/api/v1/products",
        params={"offset": 0, "limit": 100},  # Lấy 100 để filter xuống 50
        timeout=10
    )
    response.raise_for_status()
    products = response.json()

    records = []
    for item in products:
        # Lọc bỏ các sản phẩm có data không hợp lệ
        if not item.get("title") or item.get("price", 0) <= 0:
            continue

        if len(records) >= 50:  # Dừng khi đạt 50 records
            break

        records.append({
            "source": "platzi",
            "product_id": item["id"],
            "name": item["title"],
            "price": float(item["price"]),
            "currency": "USD",
            "category": item.get("category", {}).get("name", "unknown"),
            "description": item.get("description", ""),
            "images": item.get("images", []),
            "timestamp": time.time()
        })

    return records[:50]  # Đảm bảo đúng 50 records


def extract_and_send():
    producer = create_kafka_producer()
    all_records = []

    # Định nghĩa các crawl functions
    crawl_functions = [
        ("DummyJSON", crawl_dummyjson),
        ("FakeStoreAPI", crawl_fakestoreapi),
        ("Platzi Fake Store", crawl_platzi_fakestore)
    ]

    # Sử dụng ThreadPoolExecutor để crawl song song (NHANH NHẤT)
    start_time = time.time()
    print(f"Starting parallel crawl from {len(crawl_functions)} APIs...")

    with ThreadPoolExecutor(max_workers=3) as executor:
        # Submit tất cả tasks đồng thời
        future_to_api = {
            executor.submit(func): name
            for name, func in crawl_functions
        }

        # Collect results khi hoàn thành
        for future in as_completed(future_to_api):
            api_name = future_to_api[future]
            try:
                records = future.result()
                all_records.extend(records)
                print(f"✓ {api_name}: {len(records)} products")
            except Exception as e:
                print(f"✗ {api_name} error: {str(e)}")

    elapsed_time = time.time() - start_time
    print(f"Crawled {len(all_records)} total records in {elapsed_time:.2f} seconds")

    # Send tất cả records to Kafka
    print(f"Sending {len(all_records)} records to Kafka...")
    for record in all_records:
        producer.send(KAFKA_TOPIC, value=record)

    producer.flush()
    producer.close()
    print("Done!")


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
        dag_id="extract_ecommerce_api_to_kafka",
        default_args=default_args,
        description="Crawl products from 3 APIs in parallel every 3 minutes",
        schedule_interval="*/3 * * * *",  # Chạy mỗi 3 phút
        catchup=False,
        max_active_runs=1,
        tags=["api", "kafka", "ecommerce", "free-api", "parallel"]
) as dag:
    extract_task = PythonOperator(
        task_id="extract_and_send_to_kafka",
        python_callable=extract_and_send,
        execution_timeout=timedelta(minutes=10), # Giới hạn thời gian thực thi
    )