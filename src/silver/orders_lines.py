from pyspark.sql import Window
from pyspark.sql import functions as F

from src.utils.delta_upsert import DeltaUpsertManager

SRC_TABLE_NAME = "catalog.bronze.sales"
SILVER_TABLE_NAME = "catalog.silver.orders_lines"

def build_silver_orders_lines(df):
    # A qué responde esta tabla:
    # ¿Qué se vendió?
    # ¿Cuánto?
    # ¿Con qué descuento?

    df_order_lines = df.select(
        F.col("Order ID").alias("order_id"),
        F.col("Product ID").alias("product_id"),
        F.col("Quantity").alias("quantity"),
        F.col("Discount").alias("discount")
    )

    return df_order_lines

def main():
    df_bronze = spark.table(SRC_TABLE_NAME)
    df_silver = build_silver_orders_lines(df_bronze)

    upsert_manager = DeltaUpsertManager(spark)
    upsert_manager.upsert_scd_type1(
        df_source=df_silver,
        target_table_name=SILVER_TABLE_NAME,
        keys=["order_id"]
    )

if __name__ == "__main__":
    main()