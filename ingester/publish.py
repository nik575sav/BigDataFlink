import csv
import glob
import json
import os
import time
import random
from pathlib import Path

from kafka import KafkaProducer

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
OUT_TOPIC = os.getenv("KAFKA_TOPIC", "raw_sales")
SOURCE_DIR = Path(os.getenv("DATA_DIR", "./data"))
BASE_DELAY = float(os.getenv("SEND_DELAY_SECONDS", "0.01"))

def find_csv_files(dir_path: Path) -> list[str]:
    patterns = [str(dir_path / "*.csv")]
    candidates = []
    for pat in patterns:
        candidates.extend(glob.glob(pat))
    sales_files = sorted([f for f in candidates if "MOCK_DATA" in os.path.basename(f)])
    if not sales_files:
        raise FileNotFoundError(f"Нет файлов MOCK_DATA*.csv в {dir_path}")
    return sales_files

def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8") if k else None,
        acks="all",
        retries=5,
    )

def clean_row(row: dict) -> dict:
    cleaned = {}
    for key, val in row.items():
        if val is None:
            cleaned[key] = None
            continue
        val = val.strip()
        cleaned[key] = val if val != "" else None
    return cleaned

def stream_to_kafka(producer: KafkaProducer, file_list: list[str]) -> int:
    total = 0
    for fpath in file_list:
        print(f"Чтение {fpath}")
        with open(fpath, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for raw in reader:
                msg = clean_row(raw)
                record_id = msg.get("id")
                future = producer.send(OUT_TOPIC, key=record_id, value=msg)
                future.get(timeout=30)
                total += 1
                if total % 500 == 0:
                    print(f"Отправлено {total}")
                if BASE_DELAY > 0:
                    time.sleep(BASE_DELAY + random.uniform(0, 0.005))
    producer.flush()
    return total

def main():
    print("=== Kafka Producer (CSV → Topic) ===")
    print(f"Сервер: {KAFKA_SERVERS}, топик: {OUT_TOPIC}, папка: {SOURCE_DIR}")
    csv_files = find_csv_files(SOURCE_DIR)
    for f in csv_files:
        print(f"  - {f}")
    prod = create_producer()
    try:
        sent = stream_to_kafka(prod, csv_files)
        print(f"Отправлено сообщений: {sent}")
    finally:
        prod.close()

if __name__ == "__main__":
    main()
