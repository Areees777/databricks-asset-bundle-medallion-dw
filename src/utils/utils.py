from pyspark.sql import Window, DataFrame
from delta.tables import DeltaTable

def upsert_sdc_type_1(df: DataFrame, table_name: str) -> None:
    table_name = "catalog.silver.customers"

    table_exists = spark.catalog.tableExists(table_name)
    if not table_exists:
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    else:
        delta_table = DeltaTable.forName(spark, table_name)
        (
            delta_table.alias("t")
            .merge(
                df.alias("s"),
                "t.customer_id = s.customer_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )