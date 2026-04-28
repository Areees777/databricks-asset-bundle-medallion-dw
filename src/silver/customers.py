from pyspark.sql import Window
from pyspark.sql import functions as F

from utils.delta_upsert import DeltaUpsertManager

SRC_TABLE_NAME = "catalog.bronze.sales"
SILVER_TABLE_NAME = "catalog.silver.customers"

def build_silver_customers(df):

    # A qué responde esta tabla: 
    # ¿Qué sabemos de un cliente?

    

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

    upsert_manager = DeltaUpsertManager(spark)
    upsert_manager.upsert_scd_type1(
        df_source=df_silver,
        target_table_name=SILVER_TABLE_NAME,
        keys=["customer_id"]
    )


if __name__ == "__main__":
    main()

# 1. Crear un .py que se encarge de crear la tabla con su schema en silver en caso de que no exista.
# 2. Crear un .py que se encargue de hacer el proceso de carga incremental (upsert) a silver.
# 3. Añadir el paso en silver.yml para que se cree la tarea de crear la tabla y que lance la carga incremental
# 4. Hacer lo mismo en gold. 