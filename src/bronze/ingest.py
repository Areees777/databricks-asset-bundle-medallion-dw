import pandas as pd


def main():
    pd_df = pd.read_excel("/Volumes/workspace/default/datasets/Retail-Supply-Chain-Sales-Dataset.xlsx")                            
    df = spark.createDataFrame(pd_df)
    df.write.format("delta").mode("append").saveAsTable("catalog.bronze.sales")
    

if __name__ == "__main__":
    main()
