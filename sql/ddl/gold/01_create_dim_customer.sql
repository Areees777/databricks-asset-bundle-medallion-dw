CREATE TABLE IF NOT EXISTS catalog.gold.dim_customer (
    customer_key BIGINT GENERATED ALWAYS AS IDENTITY,
    
    customer_id STRING,
    customer_name STRING,
    segment STRING,
    country STRING,
    city STRING,
    state STRING,    
    postal_code BIGINT,
    region STRING,
    order_date DATE,

    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)