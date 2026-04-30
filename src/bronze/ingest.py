import pandas as pd

def clean_column_names(df_pandas):
    """Reemplaza espacios y caracteres inválidos por '_'"""
    df_pandas.columns = (df_pandas.columns
                         .str.replace(' ', '_', regex=False)          # Reemplaza espacios
                         .str.replace('-', '_', regex=False)          # Reemplaza guiones
                         .str.replace('(', '', regex=False)           # Elimina paréntesis
                         .str.replace(')', '', regex=False)
                         .str.replace('/', '_', regex=False)          # Reemplaza barras
                         .str.lower()                                 # Opcional: todo minúsculas
                         )
    return df_pandas

def main():
    pd_df = pd.read_excel("/Volumes/databricks_north_europe/bronze/files/Retail-Supply-Chain-Sales-Dataset.xlsx")
    pd_df = clean_column_names(pd_df)                    
    df = spark.createDataFrame(pd_df)
    df.write.format("delta").mode("append").saveAsTable("databricks_north_europe.bronze.sales")
    

if __name__ == "__main__":
    main()
