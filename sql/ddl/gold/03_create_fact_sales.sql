-- Crear la tabla de hechos fact_sales
CREATE TABLE IF NOT EXISTS catalog.gold.fact_sales (
    -- Claves de negocio (para MERGE y trazabilidad)
    order_id STRING COMMENT 'Identificador único del pedido',
    product_id STRING COMMENT 'Identificador único del producto',
    
    -- Claves surrogate (foráneas hacia las dimensiones)
    customer_key BIGINT COMMENT 'Clave surrogate del cliente (desde dim_customer)',
    product_key BIGINT COMMENT 'Clave surrogate del producto (desde dim_product)',
    date_key INT COMMENT 'Clave surrogate de la fecha (desde dim_date, formato YYYYMMDD)',
    
    -- Métricas (hechos)
    quantity INT COMMENT 'Cantidad de unidades vendidas del producto',
    discount DECIMAL(5,2) COMMENT 'Descuento aplicado al producto (0.00 = sin descuento)',

)
USING DELTA
COMMENT 'Tabla de hechos de ventas. Grano: línea de pedido (order_id + product_id)'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Al no tener muchos datos, <100GB no vale la pena particionar. 
-- En caso de que haya muchos datos (>100GB) se podría particionar por date_key (año-mes) para mejorar el 
-- rendimiento de las consultas que filtren por rango de fechas.

-- En todo caso, se podria aplicar un z-order sobre los campos date_key y order_id para mejorar el 
-- rendimiento de las consultas que filtren por rango de fechas y por pedido, que son las más comunes en este tipo de tabla de hechos.