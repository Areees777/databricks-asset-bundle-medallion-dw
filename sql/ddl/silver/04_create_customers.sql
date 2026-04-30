-- Crear la tabla de clientes en la capa silver
CREATE TABLE IF NOT EXISTS catalog.silver.customers (
    customer_id STRING COMMENT 'Identificador único del cliente',
    customer_name STRING COMMENT 'Nombre completo del cliente',
    segment STRING COMMENT 'Segmento del cliente (ej: Consumer, Corporate, Home Office)',
    country STRING COMMENT 'País del cliente',
    city STRING COMMENT 'Ciudad del cliente',
    state STRING COMMENT 'Estado o provincia del cliente',
    postal_code STRING COMMENT 'Código postal del cliente',
    region STRING COMMENT 'Región geográfica del cliente',
    order_date DATE COMMENT 'Fecha de la última orden del cliente'
)
USING DELTA
COMMENT 'Tabla silver con información de clientes (SCD Type 1, solo último registro por cliente)'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);