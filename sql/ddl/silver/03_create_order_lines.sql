CREATE TABLE IF NOT EXISTS catalog.silver.order_lines (
    order_id STRING COMMENT 'Identificador único del pedido',
    product_id STRING COMMENT 'Identificador único del producto vendido',
    quantity INT COMMENT 'Cantidad de unidades vendidas del producto',
    discount DECIMAL(5,2) COMMENT 'Descuento aplicado al producto (ej: 0.10 = 10%)'
)
USING DELTA
COMMENT 'Tabla silver con información de líneas de pedido: productos, cantidades y descuentos'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);