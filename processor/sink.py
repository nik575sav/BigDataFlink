import hashlib
import json
import os
import zlib
from datetime import datetime

import psycopg2
from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import MapFunction

# ---------- environment ----------
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
INPUT_TOPIC = os.getenv("KAFKA_TOPIC", "raw_sales")
CONSUMER_GROUP = os.getenv("KAFKA_GROUP_ID", "flink_sales_group")

DB_HOST = os.getenv("DB_HOST", "database")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "sales_warehouse")
DB_USER = os.getenv("DB_USER", "admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "secret")

# ---------- helpers ----------
def to_int(val):
    return int(val) if val not in (None, "") else None

def to_float(val):
    return float(val) if val not in (None, "") else None

def to_date(val):
    if val in (None, ""):
        return None
    return datetime.strptime(val, "%m/%d/%Y").date()

def to_int_key(val):
    return to_int(val)  # для buyer_key, seller_key, product_key

def surrogate_key(parts):
    """Возвращает 64-битное целое на основе SHA256 (первые 8 байт)"""
    raw = "|".join("" if p is None else str(p).strip() for p in parts)
    digest = hashlib.sha256(raw.encode("utf-8")).digest()
    # Первые 8 байт как big‑endian int
    return int.from_bytes(digest[:8], byteorder='big', signed=False) % (2**63)

def month_name_en(d):
    return d.strftime("%B") if d else None

def transform_event(raw_json: str):
    rec = json.loads(raw_json)
    sale_date = to_date(rec.get("sale_date"))
    prod_release = to_date(rec.get("product_release_date"))
    prod_expiry = to_date(rec.get("product_expiry_date"))

    # размерности
    buyer_key = to_int_key(rec.get("sale_customer_id"))
    salesperson_key = to_int_key(rec.get("sale_seller_id"))
    item_key = to_int_key(rec.get("sale_product_id"))
    time_key = int(sale_date.strftime("%Y%m%d")) if sale_date else None

    # хэш-ключи для магазина и поставщика
    shop_key = surrogate_key([
        rec.get("store_name"), rec.get("store_location"),
        rec.get("store_city"), rec.get("store_state"),
        rec.get("store_country"), rec.get("store_phone"),
        rec.get("store_email")
    ])
    vendor_key = surrogate_key([
        rec.get("supplier_name"), rec.get("supplier_contact"),
        rec.get("supplier_email"), rec.get("supplier_phone"),
        rec.get("supplier_address"), rec.get("supplier_city"),
        rec.get("supplier_country")
    ])

    return {
        "transaction_id": to_int(rec.get("id")),
        "sale_date": sale_date,
        "time_key": time_key,
        "buyer_key": buyer_key,
        "salesperson_key": salesperson_key,
        "item_key": item_key,
        "shop_key": shop_key,
        "vendor_key": vendor_key,
        "quantity": to_int(rec.get("sale_quantity")),
        "total_amount": to_float(rec.get("sale_total_price")),

        # buyer
        "ext_customer_id": to_int(rec.get("sale_customer_id")),
        "buyer_first_name": rec.get("customer_first_name"),
        "buyer_last_name": rec.get("customer_last_name"),
        "buyer_age": to_int(rec.get("customer_age")),
        "buyer_email": rec.get("customer_email"),
        "buyer_country": rec.get("customer_country"),
        "buyer_zip": rec.get("customer_postal_code"),
        "buyer_pet_type": rec.get("customer_pet_type"),
        "buyer_pet_name": rec.get("customer_pet_name"),
        "buyer_pet_breed": rec.get("customer_pet_breed"),

        # salesperson
        "ext_seller_id": to_int(rec.get("sale_seller_id")),
        "sp_first_name": rec.get("seller_first_name"),
        "sp_last_name": rec.get("seller_last_name"),
        "sp_email": rec.get("seller_email"),
        "sp_country": rec.get("seller_country"),
        "sp_zip": rec.get("seller_postal_code"),

        # item
        "ext_product_id": to_int(rec.get("sale_product_id")),
        "item_name": rec.get("product_name"),
        "item_category": rec.get("product_category"),
        "item_price": to_float(rec.get("product_price")),
        "item_quantity": to_int(rec.get("product_quantity")),
        "pet_type": rec.get("pet_category"),
        "item_weight": to_float(rec.get("product_weight")),
        "item_color": rec.get("product_color"),
        "item_size": rec.get("product_size"),
        "item_brand": rec.get("product_brand"),
        "item_material": rec.get("product_material"),
        "item_desc": rec.get("product_description"),
        "item_rating": to_float(rec.get("product_rating")),
        "item_reviews": to_int(rec.get("product_reviews")),
        "item_release_date": prod_release,
        "item_expiry_date": prod_expiry,

        # shop
        "shop_name": rec.get("store_name"),
        "shop_location": rec.get("store_location"),
        "shop_city": rec.get("store_city"),
        "shop_state": rec.get("store_state"),
        "shop_country": rec.get("store_country"),
        "shop_phone": rec.get("store_phone"),
        "shop_email": rec.get("store_email"),

        # vendor
        "vendor_name": rec.get("supplier_name"),
        "vendor_contact": rec.get("supplier_contact"),
        "vendor_email": rec.get("supplier_email"),
        "vendor_phone": rec.get("supplier_phone"),
        "vendor_address": rec.get("supplier_address"),
        "vendor_city": rec.get("supplier_city"),
        "vendor_country": rec.get("supplier_country"),

        # time
        "day_of_month": sale_date.day if sale_date else None,
        "month_number": sale_date.month if sale_date else None,
        "month_label": month_name_en(sale_date) if sale_date else None,
        "quarter_number": ((sale_date.month-1)//3+1) if sale_date else None,
        "year_number": sale_date.year if sale_date else None
    }

class StarSchemaWriter(MapFunction):
    def open(self, runtime_context):
        self.conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASSWORD
        )
        self.conn.autocommit = False
        self.cur = self.conn.cursor()
        print("PostgreSQL connection established")

    def close(self):
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        print("PostgreSQL connection closed")

    def map(self, value):
        ev = transform_event(value)

        # ---------- dim_buyers ----------
        self.cur.execute("""
            INSERT INTO dim_buyers (buyer_key, ext_customer_id, buyer_first_name, buyer_last_name,
                buyer_age, buyer_email, buyer_country, buyer_zip, buyer_pet_type,
                buyer_pet_name, buyer_pet_breed)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (buyer_key) DO UPDATE SET
                ext_customer_id=EXCLUDED.ext_customer_id,
                buyer_first_name=EXCLUDED.buyer_first_name,
                buyer_last_name=EXCLUDED.buyer_last_name,
                buyer_age=EXCLUDED.buyer_age,
                buyer_email=EXCLUDED.buyer_email,
                buyer_country=EXCLUDED.buyer_country,
                buyer_zip=EXCLUDED.buyer_zip,
                buyer_pet_type=EXCLUDED.buyer_pet_type,
                buyer_pet_name=EXCLUDED.buyer_pet_name,
                buyer_pet_breed=EXCLUDED.buyer_pet_breed
        """, (ev["buyer_key"], ev["ext_customer_id"], ev["buyer_first_name"], ev["buyer_last_name"],
              ev["buyer_age"], ev["buyer_email"], ev["buyer_country"], ev["buyer_zip"],
              ev["buyer_pet_type"], ev["buyer_pet_name"], ev["buyer_pet_breed"]))

        # ---------- dim_salespersons ----------
        self.cur.execute("""
            INSERT INTO dim_salespersons (salesperson_key, ext_seller_id, sp_first_name, sp_last_name,
                sp_email, sp_country, sp_zip)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (salesperson_key) DO UPDATE SET
                ext_seller_id=EXCLUDED.ext_seller_id,
                sp_first_name=EXCLUDED.sp_first_name,
                sp_last_name=EXCLUDED.sp_last_name,
                sp_email=EXCLUDED.sp_email,
                sp_country=EXCLUDED.sp_country,
                sp_zip=EXCLUDED.sp_zip
        """, (ev["salesperson_key"], ev["ext_seller_id"], ev["sp_first_name"], ev["sp_last_name"],
              ev["sp_email"], ev["sp_country"], ev["sp_zip"]))

        # ---------- dim_items ----------
        self.cur.execute("""
            INSERT INTO dim_items (item_key, ext_product_id, item_name, item_category, item_price,
                item_quantity, pet_type, item_weight, item_color, item_size, item_brand,
                item_material, item_desc, item_rating, item_reviews, item_release_date, item_expiry_date)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (item_key) DO UPDATE SET
                ext_product_id=EXCLUDED.ext_product_id,
                item_name=EXCLUDED.item_name,
                item_category=EXCLUDED.item_category,
                item_price=EXCLUDED.item_price,
                item_quantity=EXCLUDED.item_quantity,
                pet_type=EXCLUDED.pet_type,
                item_weight=EXCLUDED.item_weight,
                item_color=EXCLUDED.item_color,
                item_size=EXCLUDED.item_size,
                item_brand=EXCLUDED.item_brand,
                item_material=EXCLUDED.item_material,
                item_desc=EXCLUDED.item_desc,
                item_rating=EXCLUDED.item_rating,
                item_reviews=EXCLUDED.item_reviews,
                item_release_date=EXCLUDED.item_release_date,
                item_expiry_date=EXCLUDED.item_expiry_date
        """, (ev["item_key"], ev["ext_product_id"], ev["item_name"], ev["item_category"],
              ev["item_price"], ev["item_quantity"], ev["pet_type"], ev["item_weight"],
              ev["item_color"], ev["item_size"], ev["item_brand"], ev["item_material"],
              ev["item_desc"], ev["item_rating"], ev["item_reviews"],
              ev["item_release_date"], ev["item_expiry_date"]))

        # ---------- dim_shops ----------
        self.cur.execute("""
            INSERT INTO dim_shops (shop_key, shop_name, shop_location, shop_city, shop_state,
                shop_country, shop_phone, shop_email)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (shop_key) DO UPDATE SET
                shop_name=EXCLUDED.shop_name,
                shop_location=EXCLUDED.shop_location,
                shop_city=EXCLUDED.shop_city,
                shop_state=EXCLUDED.shop_state,
                shop_country=EXCLUDED.shop_country,
                shop_phone=EXCLUDED.shop_phone,
                shop_email=EXCLUDED.shop_email
        """, (ev["shop_key"], ev["shop_name"], ev["shop_location"], ev["shop_city"],
              ev["shop_state"], ev["shop_country"], ev["shop_phone"], ev["shop_email"]))

        # ---------- dim_vendors ----------
        self.cur.execute("""
            INSERT INTO dim_vendors (vendor_key, vendor_name, vendor_contact, vendor_email,
                vendor_phone, vendor_address, vendor_city, vendor_country)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (vendor_key) DO UPDATE SET
                vendor_name=EXCLUDED.vendor_name,
                vendor_contact=EXCLUDED.vendor_contact,
                vendor_email=EXCLUDED.vendor_email,
                vendor_phone=EXCLUDED.vendor_phone,
                vendor_address=EXCLUDED.vendor_address,
                vendor_city=EXCLUDED.vendor_city,
                vendor_country=EXCLUDED.vendor_country
        """, (ev["vendor_key"], ev["vendor_name"], ev["vendor_contact"], ev["vendor_email"],
              ev["vendor_phone"], ev["vendor_address"], ev["vendor_city"], ev["vendor_country"]))

        # ---------- dim_time ----------
        self.cur.execute("""
            INSERT INTO dim_time (time_key, calendar_date, day_of_month, month_number,
                month_label, quarter_number, year_number)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (time_key) DO UPDATE SET
                calendar_date=EXCLUDED.calendar_date,
                day_of_month=EXCLUDED.day_of_month,
                month_number=EXCLUDED.month_number,
                month_label=EXCLUDED.month_label,
                quarter_number=EXCLUDED.quarter_number,
                year_number=EXCLUDED.year_number
        """, (ev["time_key"], ev["sale_date"], ev["day_of_month"], ev["month_number"],
              ev["month_label"], ev["quarter_number"], ev["year_number"]))

        # ---------- fact_transactions ----------
        self.cur.execute("""
            INSERT INTO fact_transactions (transaction_id, time_key, buyer_key, salesperson_key,
                item_key, shop_key, vendor_key, quantity, total_amount)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (transaction_id) DO UPDATE SET
                time_key=EXCLUDED.time_key,
                buyer_key=EXCLUDED.buyer_key,
                salesperson_key=EXCLUDED.salesperson_key,
                item_key=EXCLUDED.item_key,
                shop_key=EXCLUDED.shop_key,
                vendor_key=EXCLUDED.vendor_key,
                quantity=EXCLUDED.quantity,
                total_amount=EXCLUDED.total_amount
        """, (ev["transaction_id"], ev["time_key"], ev["buyer_key"], ev["salesperson_key"],
              ev["item_key"], ev["shop_key"], ev["vendor_key"], ev["quantity"], ev["total_amount"]))

        self.conn.commit()
        print(f"Committed transaction {ev['transaction_id']}")
        return str(ev["transaction_id"])

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(10000)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics(INPUT_TOPIC)
        .set_group_id(CONSUMER_GROUP)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "kafka-source"
    )

    stream.map(StarSchemaWriter(), output_type=Types.STRING()).print()

    env.execute("flink-star-schema-ingestion")

if __name__ == "__main__":
    main()
