-- Crear la tabla de pedidos en la capa silver
CREATE TABLE IF NOT EXISTS catalog.silver.orders (
    order_id STRING COMMENT 'Identificador único del pedido',
    order_date DATE COMMENT 'Fecha en que se realizó el pedido',
    ship_date DATE COMMENT 'Fecha en que se envió el pedido',
    ship_mode STRING COMMENT 'Método de envío (ej: Standard Class, Second Class, etc.)',
    returned STRING COMMENT 'Indica si el pedido fue devuelto (Yes/No)',
    customer_id STRING COMMENT 'Identificador único del cliente',
    sales_person_name STRING COMMENT 'Nombre del vendedor que realizó la venta'
)
USING DELTA
COMMENT 'Tabla silver con información de pedidos, incluyendo fechas, envíos y vendedores'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);