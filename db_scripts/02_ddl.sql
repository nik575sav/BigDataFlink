DROP TABLE IF EXISTS fact_transactions CASCADE;
DROP TABLE IF EXISTS dim_buyers CASCADE;
DROP TABLE IF EXISTS dim_salespersons CASCADE;
DROP TABLE IF EXISTS dim_items CASCADE;
DROP TABLE IF EXISTS dim_shops CASCADE;
DROP TABLE IF EXISTS dim_vendors CASCADE;
DROP TABLE IF EXISTS dim_time CASCADE;

CREATE TABLE dim_buyers (
    buyer_key BIGINT PRIMARY KEY,
    ext_customer_id BIGINT,
    buyer_first_name TEXT,
    buyer_last_name TEXT,
    buyer_age INT,
    buyer_email TEXT,
    buyer_country TEXT,
    buyer_zip TEXT,
    buyer_pet_type TEXT,
    buyer_pet_name TEXT,
    buyer_pet_breed TEXT
);

CREATE TABLE dim_salespersons (
    salesperson_key BIGINT PRIMARY KEY,
    ext_seller_id BIGINT,
    sp_first_name TEXT,
    sp_last_name TEXT,
    sp_email TEXT,
    sp_country TEXT,
    sp_zip TEXT
);

CREATE TABLE dim_items (
    item_key BIGINT PRIMARY KEY,
    ext_product_id BIGINT,
    item_name TEXT,
    item_category TEXT,
    item_price NUMERIC(14,2),
    item_quantity INT,
    pet_type TEXT,
    item_weight NUMERIC(14,2),
    item_color TEXT,
    item_size TEXT,
    item_brand TEXT,
    item_material TEXT,
    item_desc TEXT,
    item_rating NUMERIC(4,2),
    item_reviews INT,
    item_release_date DATE,
    item_expiry_date DATE
);

CREATE TABLE dim_shops (
    shop_key BIGINT PRIMARY KEY,
    shop_name TEXT,
    shop_location TEXT,
    shop_city TEXT,
    shop_state TEXT,
    shop_country TEXT,
    shop_phone TEXT,
    shop_email TEXT
);

CREATE TABLE dim_vendors (
    vendor_key BIGINT PRIMARY KEY,
    vendor_name TEXT,
    vendor_contact TEXT,
    vendor_email TEXT,
    vendor_phone TEXT,
    vendor_address TEXT,
    vendor_city TEXT,
    vendor_country TEXT
);

CREATE TABLE dim_time (
    time_key INT PRIMARY KEY,
    calendar_date DATE,
    day_of_month INT,
    month_number INT,
    month_label TEXT,
    quarter_number INT,
    year_number INT
);

CREATE TABLE fact_transactions (
    transaction_id BIGINT PRIMARY KEY,
    time_key INT REFERENCES dim_time(time_key),
    buyer_key BIGINT REFERENCES dim_buyers(buyer_key),
    salesperson_key BIGINT REFERENCES dim_salespersons(salesperson_key),
    item_key BIGINT REFERENCES dim_items(item_key),
    shop_key BIGINT REFERENCES dim_shops(shop_key),
    vendor_key BIGINT REFERENCES dim_vendors(vendor_key),
    quantity INT,
    total_amount NUMERIC(14,2)
);
