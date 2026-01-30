from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import json
import time
import requests
from kafka import KafkaProducer
from concurrent.futures import ThreadPoolExecutor, as_completed

KAFKA_TOPIC = "ecommerce_products"
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"


def create_kafka_producer():
    """Create Kafka producer with retry logic"""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
        max_in_flight_requests_per_connection=5,
        request_timeout_ms=30000,
        api_version=(0, 10, 1)
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
            "limit": 80,  # số items
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
            "product_id": str(item["id"]),  # Convert to string for consistency
            "name": item["title"],
            "price": float(item["price"]),
            "currency": "USD",
            "category": item.get("category", "unknown"),
            "brand": item.get("brand", "unknown"),
            "rating": float(item.get("rating", 0)),
            "stock": int(item.get("stock", 0)),
            "thumbnail": item.get("thumbnail", ""),
            "timestamp": time.time()
        })

    return records


def crawl_fakestoreapi():
    """
    FakeStoreAPI - Free fake REST API for e-commerce
    Không cần đăng ký hoặc API key
    Có 20 sản phẩm với đầy đủ thông tin
    Strategy: Lấy tất cả products và duplicate với variation để đạt 100 records
    """
    response = requests.get(
        "https://fakestoreapi.com/products",
        timeout=10
    )
    response.raise_for_status()
    products = response.json()

    records = []

    # Lặp lại products để đạt 100 records với slight variations
    for round_num in range(6):  # 6 rounds
        for item in products:
            if len(records) >= 100:  # Giới hạn 100 records
                break

            records.append({
                "source": "fakestoreapi",
                "product_id": f"{item['id']}_r{round_num}",  # Unique ID mỗi round (already string)
                "name": item["title"],
                "price": float(item["price"]) * (1 + round_num * 0.05),  # Price variation
                "currency": "USD",
                "category": item.get("category", "unknown"),
                "rating": float(item.get("rating", {}).get("rate", 0)),
                "rating_count": int(item.get("rating", {}).get("count", 0)),
                "description": item.get("description", ""),
                "image": item.get("image", ""),
                "timestamp": time.time()
            })

        if len(records) >= 100:
            break

    return records[:100]


def crawl_platzi_fakestore():
    """
    Platzi Fake Store API - Free fake REST API
    Không cần đăng ký hoặc API key
    Hỗ trợ filtering và có nhiều sản phẩm
    Strategy: Lấy nhiều products và filter để đạt đúng 20 valid records
    """
    response = requests.get(
        "https://api.escuelajs.co/api/v1/products",
        params={"offset": 0, "limit": 150},
        timeout=10
    )
    response.raise_for_status()
    products = response.json()

    records = []
    for item in products:
        # Lọc bỏ các sản phẩm có data không hợp lệ
        if not item.get("title") or item.get("price", 0) <= 0:
            continue

        if len(records) >= 20:  # Dừng khi đạt 20 records
            break

        records.append({
            "source": "platzi",
            "product_id": str(item["id"]),  # Convert to string
            "name": item["title"],
            "price": float(item["price"]),
            "currency": "USD",
            "category": item.get("category", {}).get("name", "unknown") if isinstance(item.get("category"), dict) else "unknown",
            "description": item.get("description", ""),
            "images": item.get("images", []) if isinstance(item.get("images"), list) else [],
            "timestamp": time.time()
        })

    return records[:20]


def extract_and_send():
    """Main extraction and sending function"""
    producer = create_kafka_producer()
    all_records = []

    # Định nghĩa các crawl functions
    crawl_functions = [
        ("DummyJSON", crawl_dummyjson),
        ("FakeStoreAPI", crawl_fakestoreapi),
        ("Platzi Fake Store", crawl_platzi_fakestore)
    ]

    # Tracking metrics cho từng API
    api_metrics = {}

    # Sử dụng ThreadPoolExecutor để crawl song song
    overall_start = time.time()
    print("=" * 70)
    print(f"--->Starting parallel crawl from {len(crawl_functions)} APIs...")
    print("=" * 70)

    with ThreadPoolExecutor(max_workers=3) as executor:
        # Submit tất cả tasks đồng thời và track start time
        future_to_api = {}
        for name, func in crawl_functions:
            future = executor.submit(func)
            future_to_api[future] = {
                "name": name,
                "start_time": time.time()
            }

        # Collect results khi hoàn thành
        for future in as_completed(future_to_api):
            api_info = future_to_api[future]
            api_name = api_info["name"]
            start_time = api_info["start_time"]

            try:
                records = future.result()
                end_time = time.time()
                elapsed = end_time - start_time

                all_records.extend(records)
                api_metrics[api_name] = {
                    "records": len(records),
                    "time": elapsed,
                    "status": "success"
                }

                print(f"✅ {api_name}:")
                print(f"   └─ Records: {len(records)} products")
                print(f"   └─ Time: {elapsed:.2f}s")
                print(f"   └─ Speed: {len(records) / elapsed:.1f} products/sec")

            except Exception as e:
                end_time = time.time()
                elapsed = end_time - start_time

                api_metrics[api_name] = {
                    "records": 0,
                    "time": elapsed,
                    "status": "failed",
                    "error": str(e)
                }

                print(f"❌ {api_name}:")
                print(f"   └─ Error: {str(e)}")
                print(f"   └─ Time: {elapsed:.2f}s")

    overall_elapsed = time.time() - overall_start

    # Print summary statistics
    print("=" * 70)
    print("CRAWL SUMMARY:")
    print("=" * 70)

    total_records = len(all_records)
    successful_apis = sum(1 for m in api_metrics.values() if m["status"] == "success")

    for api_name, metrics in api_metrics.items():
        status_icon = "✅" if metrics["status"] == "success" else "❌"
        print(f"{status_icon} {api_name:20} | {metrics['records']:3} records | {metrics['time']:5.2f}s")

    print("-" * 70)
    print(f"Total Records:     {total_records}")
    print(f"Total Time:        {overall_elapsed:.2f}s")
    print(f"Success Rate:      {successful_apis}/{len(crawl_functions)} APIs")
    print(f"Overall Speed:     {total_records / overall_elapsed:.1f} products/sec")
    print(f"Parallel Speedup:  ~{sum(m['time'] for m in api_metrics.values()) / overall_elapsed:.1f}x faster")
    print("=" * 70)

    # Send tất cả records to Kafka
    if total_records == 0:
        print("⚠️  No records to send to Kafka")
        producer.close()
        return

    kafka_start = time.time()
    print(f"-->Sending {total_records} records to Kafka topic '{KAFKA_TOPIC}'...")

    sent_count = 0
    failed_count = 0

    for record in all_records:
        try:
            future = producer.send(KAFKA_TOPIC, value=record)
            # Wait for acknowledgment with timeout
            future.get(timeout=10)
            sent_count += 1
        except Exception as e:
            failed_count += 1
            print(f"Failed to send record: {e}")

    producer.flush()
    producer.close()

    kafka_elapsed = time.time() - kafka_start
    print(f"Kafka send completed in {kafka_elapsed:.2f}s")
    print(f"   └─ Sent: {sent_count}/{total_records} messages")
    print(f"   └─ Failed: {failed_count}")
    print(f"   └─ Speed: {sent_count / kafka_elapsed:.1f} msgs/sec")

    total_pipeline_time = time.time() - overall_start
    print("=" * 70)
    print(f"PIPELINE COMPLETED in {total_pipeline_time:.2f}s")
    print("=" * 70)


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 1, 30),
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": False,
}

with DAG(
        dag_id="extract_ecommerce_api_to_kafka",
        default_args=default_args,
        description="Crawl products from 3 APIs in parallel every 3 minutes",
        schedule_interval="*/3 * * * *",  # Changed from */1 to */3 (every 3 minutes)
        start_date=days_ago(1),
        catchup=False,
        max_active_runs=1,
        tags=["api", "kafka", "ecommerce", "free-api", "parallel"]
) as dag:
    extract_task = PythonOperator(
        task_id="extract_and_send_to_kafka",
        python_callable=extract_and_send,
        execution_timeout=timedelta(minutes=10),
    )