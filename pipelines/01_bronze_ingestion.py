# ==============================================================================
#Este flujo se hizo realmente con un pipeline en Fabric como primer paso.
# PIPELINE: 01_Bronze_Ingestion 
# Objetivo: Ingesta de tablas transaccionales y maestras desde Fabric SQL DB
#           hacia la capa Bronze Lakehouse (LH_Bronze) en formato Delta Lake.
# Método: Copy Activity / Orchestrated Ingestion Pattern
# ==============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

# Lista de tablas a ingerir desde el origen
TABLAS_ORIGEN = [
    "MSTR_PROVEEDORES",
    "MSTR_ARTICULOS",
    "MSTR_TIENDAS",
    "CRM_MIEMBROS",
    "TRANS_VENTAS",
    "INV_STOCK_DIARIO",
    "POST_DEVOLUCIONES"
]

# Configuración genérica de Lakehouse Bronze
PATH_BRONZE_LAKEHOUSE = "abfss://<WORKSPACE_ID>@onelake.dfs.fabric.microsoft.com/<LAKEHOUSE_ID>/Tables/Bronze"

def ingest_to_bronze(tabla):
    """
    Lee la tabla origen en la base de datos SQL de Fabric y la persiste 
    en formato Delta Lake en la capa Bronze añadiendo metadatos de auditoría.
    """
    print(f"Iniciando ingesta raw para la tabla: {tabla}...")
    
    # Lectura de la tabla desde la zona de landing / SQL Database
    df_raw = spark.read.table(f"landing_db.{tabla}")
    
    # Adición de metadatos de linaje y auditoría
    df_bronze = df_raw.withColumn("_ingestion_timestamp", current_timestamp()) \
                       .withColumn("_source_system", lit("FABRIC_SQL_DB"))
    
    # Escritura en capa Bronze (Delta Lake)
    df_bronze.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(f"{PATH_BRONZE_LAKEHOUSE}/{tabla.lower()}")
        
    print(f"Tabla {tabla} ingerida exitosamente en Bronze.")

if __name__ == "__main__":
    for tabla in TABLAS_ORIGEN:
        ingest_to_bronze(tabla)
