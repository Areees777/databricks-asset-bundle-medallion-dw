from pyspark.sql import Window
from pyspark.sql import functions as F

from src.utils.delta_upsert import DeltaUpsertManager

SRC_TABLE_NAME = "catalog.bronze.sales"
SILVER_TABLE_NAME = "catalog.silver.orders"

def build_silver_orders(df):
    # A qué responde esta tabla:
    # ¿Qué es un pedido y cuando ocurrió?
    # ¿Cuándo se hizo el pedido?
    # ¿A quién?
    # ¿Cómo se envió?
    # ¿Fue devuelto?
    # ¿Quien vendió el producto?

    df_orders = df.select(
        F.col("Order ID").alias("order_id"),
        F.col("Order Date").alias("order_date"),
        F.col("Ship Date").alias("ship_date"),
        F.col("Ship Mode").alias("ship_mode"),
        F.col("Returned").alias("returned"),
        F.col("Customer ID").alias("customer_id"),
        F.col("Retail Sales People").alias("sales_person_name")
    )

    df_orders_cleaned = df_orders.dropDuplicates(["order_id"])

    return df_orders_cleaned

def main():
    df_bronze = spark.table(SRC_TABLE_NAME)
    df_silver = build_silver_orders(df_bronze)

    upsert_manager = DeltaUpsertManager(spark)
    upsert_manager.upsert_scd_type1(
        df_source=df_silver,
        target_table_name=SILVER_TABLE_NAME,
        keys=["order_id"]
    )

if __name__ == "__main__":
    main()