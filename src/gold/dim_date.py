from pyspark.sql import Window
from pyspark.sql import functions as F

START_DATE = "1992-01-01"
END_DATE = "2026-12-31"

def main():
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

    df_dim_date.write.format("delta").mode("overwrite").saveAsTable("catalog.gold.dim_date")

if __name__ == "__main__":
    main()