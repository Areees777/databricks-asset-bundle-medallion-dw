from pyspark.sql import functions as F
from pyspark.sql import Window, SparkSession, DataFrame
from delta.tables import DeltaTable
from typing import Dict, List, Optional
from pyspark.sql.column import Column

class DeltaUpsertManager:
    """
    Clase para manejar operaciones de upsert en tablas Delta de manera genérica.
    Implementa SCD Type 2
    """

    def __init__(self, spark: SparkSession):
            self.spark = spark

    @staticmethod
    def build_merge_condition(keys: List[str], columns_to_track: List[str] = None, custom_merge_condition: str = None) -> str:
        """
        Construye la condición para el MERGE.
        
        Args:
            keys: Lista de columnas que forman la clave primaria
            columns_to_track: Condicion para comprobar cambios en los datos
        
        Returns:
            Condición SQL para el MERGE
        """
        key_conditions = [f"t.{key} = s.{key}" for key in keys]
        condition = " AND ".join(key_conditions)

        change_conditions = []
        if columns_to_track:
            for col in columns_to_track:
                change_conditions.append(f"t.{col} <> s.{col}")

            change_condition = " OR ".join(change_conditions)
            condition = f"({condition}) AND ({change_condition})"

        if custom_merge_condition:
            condition = f"{condition} AND ({custom_merge_condition})"
        
        return condition


    @staticmethod
    def build_update_set(columns_to_update: Optional[List[str]] = None, custom_updates: Optional[Dict[str, Column]] = None) -> Dict[str, Column]:
        """
        Construye el diccionario de actualizaciones para whenMatchedUpdate.
        
        Args:
            columns_to_update: Columnas a actualizar cuando hay match
            custom_updates: Actualizaciones personalizadas adicionales
        
        Returns:
            Diccionario para el update
        """
        updates = {}
        
        # Configuración estándar para SCD Type 2
        if columns_to_update:
            updates.update({
                col: F.lit(None) for col in columns_to_update
            })
        
        # Actualizaciones por defecto para SCD Type 2
        updates.update({
            "valid_to": F.current_date(),
            "is_current": F.lit(False)
        })
        
        # Sobrescribir con actualizaciones personalizadas si existen
        if custom_updates:
            updates.update(custom_updates)
        
        return updates

    def upsert_scd_type2(
        self, 
        df_source: DataFrame, 
        target_table_name: str,
        keys: List[str],
        column_to_track: List[str],
        order_by_column: str = "order_date",
        valid_from_column: str = "order_date",
        custom_merge_condition: str = "t.is_current = true"
        ) -> None:
        
        merge_cond = self.build_merge_condition(keys, column_to_track, custom_merge_condition)
        update_set = self.build_update_set()

        delta_table = DeltaTable.forName(spark, target_table_name)
        (   
            delta_table.alias("t")
            .merge(
                df_source.alias("s"),
                merge_cond
            )
            .whenMatchedUpdate(set=update_set)
            .execute()
        )

        # Insertamos los nuevos registros
        window = Window.partitionBy(*keys).orderBy(order_by_column)
        df_target = spark.table(target_table_name).filter(F.col("is_current") == True)
        df_new_records = df_source.join(df_target, keys, "left_anti") \
            .withColumn("valid_from", F.col(valid_from_column)) \
            .withColumn("valid_to", F.coalesce(F.lead("order_date").over(window), F.lit('9999-12-31'))) \
            .withColumn("is_current", F.col("valid_to") == F.lit("9999-12-31").cast("date"))
        
        df_new_records.write.format("delta") \
            .mode("append") \
            .saveAsTable(target_table_name)

    def upsert_scd_type1(
        self, 
        df_source: DataFrame, 
        target_table_name: str,
        keys: List[str]
    ):
        merge_cond = self.build_merge_condition(keys)

        source_columns = df_source.columns
        updates = {col: F.col(f"s.{col}") for col in source_columns if col not in keys}
        inserts = {col: F.col(f"s.{col}") for col in source_columns}

        delta_table = DeltaTable.forName(spark, target_table_name)
        (
            delta_table.alias("t")
            .merge(
                df_source.alias("s"),
                merge_cond
            )
            .whenMatchedUpdate(set=updates)
            .whenNotMatchedInsert(values=inserts)
            .execute()
        )