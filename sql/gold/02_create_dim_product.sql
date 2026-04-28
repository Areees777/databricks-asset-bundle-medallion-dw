CREATE TABLE IF NOT EXISTS catalog.gold.dim_product (
    product_key BIGINT GENERATED ALWAYS AS IDENTITY,

    product_id STRING, 
    product_name STRING, 
    category STRING,
    sub_category STRING
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)