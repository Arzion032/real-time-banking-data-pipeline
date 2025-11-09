import boto3
from kafka import KafkaConsumer
import json
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

# -----------------------------
# Load secrets from .env
# -----------------------------
load_dotenv()

# Kafka Topics
topics = [
    'banking_server.public.customers',
    'banking_server.public.accounts',
    'banking_server.public.transactions',
    'banking_server.public.ledger_entries'
    ]

# Kafka consumer settings
consumer = KafkaConsumer(
    *topics,
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP"),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='banking-consumer-old-5',
)

# MinIO client
s3 = boto3.client(
    's3',
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY")
)

bucket = os.getenv("MINIO_BUCKET")

# Create bucket if not exists
if bucket not in [b['Name'] for b in s3.list_buckets()['Buckets']]:
    s3.create_bucket(Bucket=bucket)

# Consume and write function
def write_to_minio(table_name, records):
    if not records:
        return
    df = pd.DataFrame(records)
    date_str = datetime.now().strftime('%Y-%m-%d')
    file_path = f'{table_name}_{date_str}.parquet'
    df.to_parquet(file_path, engine='fastparquet', index=False)
    s3_key = f'{table_name}/date={date_str}/{table_name}_{datetime.now().strftime("%H%M%S%f")}.parquet'
    s3.upload_file(file_path, bucket, s3_key)
    os.remove(file_path)
    print(f'✅ Uploaded {len(records)} records to s3://{bucket}/{s3_key}')

# Default batch size
default_batch_size = 50

# Custom batch sizes per topic
batch_sizes = {
    'banking_server.public.customers': 10,
    'banking_server.public.accounts': 50,
    'banking_server.public.transactions': 50,
    'banking_server.public.ledger_entries': 50
}

buffer = {
    'banking_server.public.customers': [],
    'banking_server.public.accounts': [],
    'banking_server.public.transactions': [],
    'banking_server.public.ledger_entries': []
}

print("✅ Connected to Kafka. Listening for messages...")

for message in consumer:
    topic = message.topic
    event = json.loads(message.value.decode('utf-8'))
    payload = event.get("payload") or event
    record = payload.get("after")
    
    if record:
        buffer[topic].append(record)
        print(f"[{topic}] New record -> {record}")

    # Determine batch size for this topic
    topic_batch_size = batch_sizes.get(topic, default_batch_size)

    if len(buffer[topic]) >= topic_batch_size:
        write_to_minio(topic.split('.')[-1], buffer[topic])
        buffer[topic] = []
        print(f"[{topic}] Flushed {topic_batch_size} records to MinIO")