from pyspark.sql import Window
from pyspark.sql import functions as F

from src.utils.delta_upsert import DeltaUpsertManager

SRC_TABLE_NAME = "databricks_north_europe.bronze.sales"
SILVER_TABLE_NAME = "databricks_north_europe.silver.products"

def build_silver_products(df):

    # A qué responde esta tabla: 
    # ¿Qué productos existen?
    # ¿Qué es un producto?

    df_products = df.select(
        F.col("Product ID").alias("product_id"),
        F.col("Product Name").alias("product_name"),
        F.col("Category").alias("category"),
        F.col("Sub-Category").alias("sub_category")
    )

    df_products_cleaned = df_products.dropDuplicates(["product_id"])

    return df_products_cleaned

def main():    
    df_bronze = spark.table(SRC_TABLE_NAME)
    df_silver = build_silver_products(df_bronze)

    upsert_manager = DeltaUpsertManager(spark)
    upsert_manager.upsert_scd_type1(
        df_source=df_silver,
        target_table_name=SILVER_TABLE_NAME,
        keys=["product_id"]
    )

if __name__ == "__main__":
    main()