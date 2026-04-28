from pyspark.sql import Window
from pyspark.sql import functions as F

def main():
    df_order_lines = spark.table("catalog.silver.order_lines")
    df_orders = spark.table("catalog.silver.orders")

    dim_customer = spark.table("catalog.gold.dim_customer")
    dim_product  = spark.table("catalog.gold.dim_product")
    dim_date     = spark.table("catalog.gold.dim_date")

    df_fact_base = (
        df_order_lines.alias("ol")
        .join(
            df_orders.alias("o"),
            on="order_id",
            how="inner"
        )
    )

    join_cond_cust = ((F.col("f.customer_id") == F.col("dc.customer_id")) & 
                    (F.col("f.order_date").between(F.col("dc.valid_from"),F.col("dc.valid_to"))))

    df_fact_sales_prod = df_fact_base.alias("f") \
        .join(dim_product.select("product_key", "product_id").alias("dp"), "product_id", "inner") \
        .join(dim_customer.alias("dc"), join_cond_cust, "inner") \
        .join(dim_date.alias("dd"), F.col("f.order_date") == F.col("dd.date"), "inner") \
        .select(
            "f.product_id",
            "f.order_id",
            "dc.customer_key",
            "dp.product_key",
            "dd.date_key",
            "f.quantity",
            "f.discount"
        )

if __name__ == "__main__":
    main()