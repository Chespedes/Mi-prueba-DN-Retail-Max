from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType, StringType, BooleanType
import datetime

# -------------------------------------------------------------------------
# 0. INICIALIZACIÓN
# -------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("RetailMax_Silver_Layer_Processing") \
    .getOrCreate()

# Ruta de origen Bronze en OneLake (Parametrizada)
BRONZE_PATH = "abfss://<WORKSPACE_NAME>@onelake.dfs.fabric.microsoft.com/LH_Bronze.Lakehouse/Files/raw/"

execution_time = datetime.datetime.now()
execution_id = f"EXEC_{execution_time.strftime('%Y%m%d_%H%M%S')}"


def log_quality_metric(table_name, total_records, accepted_records, rejected_records, null_counts_dict):
    """Guarda las métricas de calidad y auditoría por cada tabla procesada."""
    pct_conformed = (accepted_records / total_records * 100) if total_records > 0 else 0.0
    
    report_df = spark.createDataFrame([(
        execution_id,
        table_name,
        total_records,
        accepted_records,
        rejected_records,
        round(pct_conformed, 2),
        str(null_counts_dict),
        execution_time
    )], [
        "execution_id", "table_name", "total_records", "accepted_records", 
        "rejected_records", "pct_conformed", "null_analysis", "processed_at"
    ])
    
    report_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("data_quality_metrics")


# -------------------------------------------------------------------------
# 1. CRM_MIEMBROS (Deduplicación, PII Hash, Imputación)
# -------------------------------------------------------------------------
df_miembros_raw = spark.read \
    .option("recursiveFileLookup", "true") \
    .format("parquet") \
    .load(f"{BRONZE_PATH}CRM_MIEMBROS")
total_miembros = df_miembros_raw.count()

# Deduplicación exacta por llave primaria
df_miembros_clean = df_miembros_raw.dropDuplicates(["id_miembro"])

# Imputación de rango_edad nulo por ventana sobre canal preferido
window_canal = Window.partitionBy("canal_pref")
df_miembros_clean = df_miembros_clean.withColumn(
    "rango_edad",
    F.coalesce(F.col("rango_edad"), F.first("rango_edad", ignorenulls=True).over(window_canal), F.lit("30-45"))
)

# Estandarización de género
df_miembros_clean = df_miembros_clean.withColumn(
    "genero",
    F.when(F.col("genero").isin(["M", "Masculino"]), "M")
     .when(F.col("genero").isin(["F", "Femenino"]), "F")
     .otherwise("No informado")
)

# Aplicación de Hashing SHA-256 para PII, formateo de fechas y cast booleano
df_miembros_silver = df_miembros_clean \
    .withColumn("id_miembro_hash", F.sha2(F.col("id_miembro").cast("string"), 256)) \
    .withColumn("fec_registro", F.to_date("fec_registro")) \
    .withColumn("fec_ultima_compra", F.to_date("fec_ultima_compra")) \
    .withColumn("activo", F.coalesce(F.col("activo").cast("boolean"), F.lit(True)))

# Guardar en Delta Lake (Tables)
df_miembros_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("crm_miembros")

accepted_m = df_miembros_silver.count()
log_quality_metric("CRM_MIEMBROS", total_miembros, accepted_m, total_miembros - accepted_m, {"rango_edad_imputados": total_miembros - accepted_m})


# -------------------------------------------------------------------------
# 2. MSTR_ARTICULOS & MSTR_PROVEEDORES
# -------------------------------------------------------------------------
df_art_raw = spark.read \
    .option("recursiveFileLookup", "true") \
    .format("parquet") \
    .load(f"{BRONZE_PATH}MSTR_ARTICULOS") \
    .dropDuplicates(["art_id"])

df_art_silver = df_art_raw \
    .withColumn("precio_lista", F.col("precio_lista").cast(DoubleType())) \
    .withColumn("peso_kg", F.col("peso_kg").cast(DoubleType())) \
    .withColumn("fec_alta", F.to_date("fec_alta")) \
    .withColumn("activo", F.coalesce(F.col("activo").cast("boolean"), F.lit(True)))

df_art_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("mstr_articulos")

df_prov_raw = spark.read \
    .option("recursiveFileLookup", "true") \
    .format("parquet") \
    .load(f"{BRONZE_PATH}MSTR_PROVEEDORES") \
    .dropDuplicates(["id_proveedor"])

df_prov_silver = df_prov_raw \
    .withColumn("tiempo_repo_dias", F.col("tiempo_repo_dias").cast(IntegerType())) \
    .withColumn("calificacion_calidad", F.col("calificacion_calidad").cast(DoubleType()))

df_prov_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("mstr_proveedores")


# -------------------------------------------------------------------------
# 3. MSTR_TIENDAS
# -------------------------------------------------------------------------
df_tiendas_raw = spark.read \
    .option("recursiveFileLookup", "true") \
    .format("parquet") \
    .load(f"{BRONZE_PATH}MSTR_TIENDAS") \
    .dropDuplicates(["id_tienda"])

df_tiendas_silver = df_tiendas_raw \
    .withColumn("tipo_tienda", F.upper(F.trim(F.col("tipo_tienda")))) \
    .withColumn("metros_cuadrados", F.col("metros_cuadrados").cast(DoubleType())) \
    .withColumn("fec_apertura", F.to_date("fec_apertura"))

df_tiendas_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("mstr_tiendas")


# -------------------------------------------------------------------------
# 4. TRANS_VENTAS (Integridad Referencial y Mapeo de Errores)
# -------------------------------------------------------------------------
df_ventas_raw = spark.read \
    .option("recursiveFileLookup", "true") \
    .format("parquet") \
    .load(f"{BRONZE_PATH}TRANS_VENTAS")
total_ventas = df_ventas_raw.count()

# Normalización de tipos
df_ventas_clean = df_ventas_raw.dropDuplicates(["id_trans"]) \
    .withColumn("fec_trans", F.to_date("fec_trans")) \
    .withColumn("qty_vendida", F.col("qty_vendida").cast(IntegerType())) \
    .withColumn("precio_unitario_venta", F.col("precio_unitario_venta").cast(DoubleType())) \
    .withColumn("descuento_aplicado", F.coalesce(F.col("descuento_aplicado").cast(DoubleType()), F.lit(0.0))) \
    .withColumn("vr_total_trans", (F.col("qty_vendida") * F.col("precio_unitario_venta")) - F.col("descuento_aplicado"))

# Listas de Llaves Válidas para Integridad Referencial
valid_articulos = df_art_silver.select("art_id").distinct()
valid_tiendas = df_tiendas_silver.select("id_tienda").distinct()

# Filtrado de Registros Válidos
df_ventas_validas = df_ventas_clean \
    .join(valid_articulos, "art_id", "inner") \
    .join(valid_tiendas, "id_tienda", "inner")

# Aislamiento de Registros Huérfanos
df_ventas_rechazadas = df_ventas_clean.join(df_ventas_validas, ["id_trans"], "left_anti") \
    .withColumn("motivo_rechazo", F.lit("ERROR_INTEGRIDAD_REFERENCIAL_ART_O_TIENDA")) \
    .withColumn("execution_id", F.lit(execution_id)) \
    .withColumn("rejected_at", F.current_timestamp())

# Guardar Rechazos en Tabla de Auditoría
df_ventas_rechazadas.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("silver_rejected_records")

# Guardar Ventas Conformes
df_ventas_validas.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("trans_ventas")

accepted_v = df_ventas_validas.count()
rejected_v = df_ventas_rechazadas.count()
log_quality_metric("TRANS_VENTAS", total_ventas, accepted_v, rejected_v, {"huerfanos_referenciales": rejected_v})


# -------------------------------------------------------------------------
# 5. INV_STOCK_DIARIO
# -------------------------------------------------------------------------
df_stock_raw = spark.read \
    .option("recursiveFileLookup", "true") \
    .format("parquet") \
    .load(f"{BRONZE_PATH}INV_STOCK_DIARIO") \
    .dropDuplicates(["id_snapshot"])

df_stock_silver = df_stock_raw \
    .withColumn("fec_snapshot", F.to_date("fec_snapshot")) \
    .withColumn("stock_fisico", F.coalesce(F.col("stock_fisico").cast(IntegerType()), F.lit(0))) \
    .withColumn("stock_transito", F.coalesce(F.col("stock_transito").cast(IntegerType()), F.lit(0))) \
    .withColumn("stock_reservado", F.coalesce(F.col("stock_reservado").cast(IntegerType()), F.lit(0)))

df_stock_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("inv_stock_diario")


# -------------------------------------------------------------------------
# 6. POST_DEVOLUCIONES
# -------------------------------------------------------------------------
df_dev_raw = spark.read \
    .option("recursiveFileLookup", "true") \
    .format("parquet") \
    .load(f"{BRONZE_PATH}POST_DEVOLUCIONES") \
    .dropDuplicates(["id_devolucion"])

df_dev_silver = df_dev_raw \
    .withColumn("fec_devolucion", F.to_date("fec_devolucion")) \
    .withColumn("qty_devuelta", F.col("qty_devuelta").cast(IntegerType())) \
    .withColumn("vr_reembolso", F.col("vr_reembolso").cast(DoubleType())) \
    .withColumn("motivo_cod", F.trim(F.upper(F.col("motivo_cod"))))

df_dev_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("post_devoluciones")

print(f"¡Procesamiento de Capa Silver finalizado con éxito! Ejecución: {execution_id}")
