-- Crear la tabla de productos en la capa silver
CREATE TABLE IF NOT EXISTS databricks_north_europe.silver.products (
    product_id STRING COMMENT 'Identificador único del producto',
    product_name STRING COMMENT 'Nombre del producto',
    category STRING COMMENT 'Categoría del producto',
    sub_category STRING COMMENT 'Subcategoría del producto',
    updated_at TIMESTAMP COMMENT 'Fecha y hora de última actualización'
)
USING DELTA
COMMENT 'Tabla silver con información de productos'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);