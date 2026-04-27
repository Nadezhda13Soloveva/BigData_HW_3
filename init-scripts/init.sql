DROP TABLE IF EXISTS fact_sales CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;
DROP TABLE IF EXISTS dim_seller CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_store CASCADE;
DROP TABLE IF EXISTS dim_supplier CASCADE;


CREATE TABLE dim_customer (
    customer_id INTEGER PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    age INTEGER,
    email TEXT,
    country TEXT,
    postal_code TEXT,
    pet_type TEXT,
    pet_name TEXT,
    pet_breed TEXT
);

CREATE TABLE dim_seller (
    seller_id INTEGER PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT,
    country TEXT,
    postal_code TEXT
);

CREATE TABLE dim_product (
    product_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT,
    price NUMERIC(10,2),
    stock_quantity INTEGER,
    weight NUMERIC(5,2),
    color TEXT,
    size TEXT,
    brand TEXT,
    material TEXT,
    description TEXT,
    rating NUMERIC(3,1),
    reviews INTEGER,
    release_date DATE,
    expiry_date DATE
);

CREATE TABLE dim_store (
    store_id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    location TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    phone TEXT,
    email TEXT
);

CREATE TABLE dim_supplier (
    supplier_id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    contact TEXT,
    email TEXT,
    phone TEXT,
    address TEXT,
    city TEXT,
    country TEXT
);

CREATE TABLE fact_sales (
    sale_id BIGSERIAL PRIMARY KEY,
    source_sale_id BIGINT,
    sale_date DATE NOT NULL,
    customer_id INTEGER,
    seller_id INTEGER,
    product_id INTEGER,
    store_id INTEGER,
    supplier_id INTEGER,
    quantity INTEGER NOT NULL CHECK (quantity >= 0),
    total_price NUMERIC(12,2) NOT NULL CHECK (total_price >= 0)
);


CREATE INDEX idx_fact_customer ON fact_sales(customer_id);
CREATE INDEX idx_fact_seller ON fact_sales(seller_id);
CREATE INDEX idx_fact_product ON fact_sales(product_id);
CREATE INDEX idx_fact_store ON fact_sales(store_id);
CREATE INDEX idx_fact_supplier ON fact_sales(supplier_id);
CREATE INDEX idx_fact_date ON fact_sales(sale_date);
CREATE INDEX idx_fact_source ON fact_sales(source_sale_id);