from pyspark.sql import Window
from pyspark.sql import functions as F

from utils.utils import upsert_sdc_type_1

SRC_TABLE_NAME = "catalog.bronze.sales"
SILVER_TABLE_NAME = "catalog.silver.customers"

def build_silver_customers(df):
    df_customers = df.select(
        F.col("Customer ID").alias("customer_id"),
        F.col("Customer Name").alias("customer_name"),
        F.col("Segment").alias("segment"),
        F.col("Country").alias("country"),
        F.col("City").alias("city"),
        F.col("State").alias("state"),
        F.col("Postal Code").alias("postal_code"),
        F.col("Region").alias("region"),
        F.col("Order Date").alias("order_date")
    )

    window = Window.partitionBy("customer_id").orderBy(F.col("order_date").desc())
    df_customers_latest = (
        df_customers
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .select(
            F.col("customer_id"),
            F.col("customer_name"),
            F.col("segment"),
            F.col("country"),
            F.col("city"),
            F.col("state"),
            F.col("postal_code"),
            F.col("region"),
            F.col("order_date").cast("date")
        )
    )

    return df_customers_latest

def main():    
    df_bronze = spark.table(SRC_TABLE_NAME)
    df_silver = build_silver_customers(df_bronze)

    upsert_sdc_type_1(df_silver, SILVER_TABLE_NAME)


if __name__ == "__main__":
    main()