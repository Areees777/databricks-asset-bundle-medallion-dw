from pyspark.sql import Window
from pyspark.sql import functions as F

from utils.delta_upsert import DeltaUpsertManager

SRC_TABLE_NAME = "databricks_north_europe.silver.customers"
TGT_TABLE_NAME = "databricks_north_europe.gold.dim_customer"

def main():
    df_source = spark.table(SRC_TABLE_NAME)

    upsert_manager = DeltaUpsertManager(spark)
    upsert_manager.upsert_scd_type2(
        df_source=df_source,
        target_table_name=TGT_TABLE_NAME,
        keys=["customer_id"],
        column_to_track=["segment", "country", "region", "state", "city", "postal_code"]
    )

if __name__ == "__main__":
    main()