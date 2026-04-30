from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

START_DATE = "1992-01-01"
END_DATE = "2026-12-31"
TGT_TABLE_NAME = "databricks_north_europe.gold.dim_date"

def table_exists(table_name):
    """Verifica si una tabla existe en Unity Catalog."""
    try:
        spark.sql(f"DESCRIBE TABLE {table_name}")
        return True
    except AnalysisException:
        return False

def main():

    if table_exists(TGT_TABLE_NAME):
        print(f"La tabla {TGT_TABLE_NAME} ya existe. No se realizará ninguna acción.")
        return
    
    df_date = spark.sql(
        f"""
        SELECT explode(sequence(to_date('{START_DATE}'), to_date('{END_DATE}'), interval 1 day)) as date
        """
    )

    df_dim_date = (
        df_date.withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .withColumn("day", F.dayofmonth("date"))
        .withColumn("week", F.weekofyear("date"))
        .withColumn("quarter", F.quarter("date"))
        .withColumn("day_of_week", F.dayofweek("date"))
        .withColumn(
            "is_weekend", F.when(F.dayofweek("date").isin([1, 7]), 1).otherwise(0)
        )
    )

    df_dim_date.write.format("delta").mode("overwrite").saveAsTable(TGT_TABLE_NAME)

if __name__ == "__main__":
    main()