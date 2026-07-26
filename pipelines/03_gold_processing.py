from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import datetime

# -------------------------------------------------------------------------
# 0. INICIALIZACIÓN
# -------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("RetailMax_Gold_Layer_Processing") \
    .getOrCreate()

execution_time = datetime.datetime.now()
execution_id = f"EXEC_GOLD_{execution_time.strftime('%Y%m%d_%H%M%S')}"

# -------------------------------------------------------------------------
# 1. LECTURA DESDE CAPA SILVER
# -------------------------------------------------------------------------
df_crm = spark.table("crm_miembros")
df_art = spark.table("mstr_articulos")
df_prov = spark.table("mstr_proveedores")
df_tiendas = spark.table("mstr_tiendas")
df_ventas = spark.table("trans_ventas")
df_stock = spark.table("inv_stock_diario")
df_devoluciones = spark.table("post_devoluciones")

# -------------------------------------------------------------------------
# 2. DIM_PRODUCTOS (Jerarquía y Margen Estimado por Categoría)
# -------------------------------------------------------------------------
df_prod_base = df_art.alias("a") \
    .join(df_prov.alias("p"), F.col("a.id_proveedor") == F.col("p.id_proveedor"), "left")

dim_productos = df_prod_base.select(
    F.col("a.art_id").alias("sk_producto"),
    F.col("a.cod_barra"),
    F.col("a.desc_art").alias("nombre_producto"),
    F.col("a.id_categ_n1").alias("categoria_nivel_1"),
    F.col("a.id_categ_n2").alias("categoria_nivel_2"),
    F.col("a.id_categ_n3").alias("categoria_nivel_3"),
    F.col("a.precio_lista"),
    F.col("a.peso_kg"),
    F.col("a.unid_medida"),
    F.col("a.activo"),
    F.col("p.id_proveedor"),
    F.col("p.razon_social").alias("nombre_proveedor"),
    F.col("p.pais_origen").alias("pais_proveedor"),
    F.col("p.tiempo_repo_dias"),
    F.round(
        F.when(F.col("a.id_categ_n1").isin(["CAT_1", "ELECTRONICA", "TEC"]), 0.25)
         .when(F.col("a.id_categ_n1").isin(["CAT_2", "ROPA", "TEXTIL"]), 0.45)
         .otherwise(0.35) * F.col("a.precio_lista"), 2
    ).alias("margen_estimado_categoria")
)

dim_productos.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dim_productos")


# -------------------------------------------------------------------------
# 3. DIM_TIENDAS (Estandarización y Corrección Geográfica)
# -------------------------------------------------------------------------
dim_tiendas_temp = df_tiendas.select(
    F.col("id_tienda").alias("sk_tienda"),
    F.col("nom_tienda").alias("nombre_tienda"),
    F.upper(F.trim(F.col("tipo_tienda"))).alias("tipo_tienda"),
    F.coalesce(F.col("id_ciudad"), F.lit("CIUDAD_PRINCIPAL")).alias("ciudad"),
    F.col("id_pais"),
    F.col("metros_cuadrados"),
    F.col("fec_apertura")
)

dim_tiendas = dim_tiendas_temp.withColumn(
    "pais",
    F.when(F.upper(F.col("ciudad")).contains("SANTIAGO"), "Chile")
     .when(F.upper(F.col("ciudad")).contains("CDMX"), "Mexico")
     .when(F.upper(F.col("ciudad")).contains("BOGOTA"), "Colombia")
     .when(F.upper(F.col("ciudad")).contains("QUITO"), "Ecuador")
     .when(F.upper(F.col("ciudad")).contains("LIMA"), "Peru")
     .otherwise(F.coalesce(F.col("id_pais"), F.lit("Colombia")))
).withColumn(
    "zona_distribucion_asignada",
    F.concat(F.lit("ZONA_DIST_"), F.upper(F.col("pais")))
).drop("id_pais")

dim_tiendas.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dim_tiendas")


# -------------------------------------------------------------------------
# 4. DIM_CLIENTES (Antigüedad en Días, Rango Edad y Género)
# -------------------------------------------------------------------------
max_fecha_proceso = df_ventas.select(F.max("fec_trans")).collect()[0][0] or datetime.date.today()

dim_clientes = df_crm.select(
    F.col("id_miembro_hash").alias("sk_cliente"),
    F.col("id_miembro").alias("id_miembro_bk"),
    F.col("genero"),
    F.col("rango_edad"),
    F.col("canal_pref"),
    F.col("fec_registro"),
    F.col("fec_ultima_compra"),
    F.col("activo"),
    F.datediff(F.lit(max_fecha_proceso), F.col("fec_registro")).alias("antiguedad_dias")
)

dim_clientes.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dim_clientes")


# -------------------------------------------------------------------------
# 5. FACT_VENTAS (Validación Clientes, Venta Neta e Indicador Descuento)
# -------------------------------------------------------------------------
fact_ventas = df_ventas.alias("v") \
    .join(df_crm.alias("c"), F.col("v.id_miembro") == F.col("c.id_miembro"), "left") \
    .select(
        F.col("v.id_trans").alias("id_transaccion"),
        F.date_format("v.fec_trans", "yyyyMMdd").cast("integer").alias("sk_tiempo"),
        F.col("v.fec_trans").alias("fec_transaccion"),
        F.col("v.art_id").alias("sk_producto"),
        F.col("v.id_tienda").alias("sk_tienda"),
        F.coalesce(F.col("c.id_miembro_hash"), F.lit("CLIENTE_ANONIMO")).alias("sk_cliente"),
        F.col("v.canal_venta"),
        F.col("v.tipo_pago"),
        F.col("v.qty_vendida").alias("unidades_vendidas"),
        F.col("v.precio_unitario_venta"),
        F.col("v.descuento_aplicado"),
        ((F.col("v.qty_vendida") * F.col("v.precio_unitario_venta")) - F.col("v.descuento_aplicado")).alias("vr_venta_neto"),
        F.when(F.col("v.descuento_aplicado") > 0, True).otherwise(False).alias("es_venta_con_descuento")
    )

fact_ventas.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("fact_ventas")


# -------------------------------------------------------------------------
# 6. FACT_INVENTARIO (Consumo 14d, Cobertura en Días y Alerta Quiebre)
# -------------------------------------------------------------------------
ventas_diarias_art = df_ventas.groupBy("art_id", "id_tienda", "fec_trans") \
    .agg(F.sum("qty_vendida").alias("qty_diaria"))

ventas_con_promedio = ventas_diarias_art.withColumn(
    "promedio_consumo_14dias",
    F.avg("qty_diaria").over(Window.partitionBy("art_id", "id_tienda"))
)

fact_inventario = df_stock.alias("s") \
    .join(
        ventas_con_promedio.alias("v"),
        (F.col("s.art_id") == F.col("v.art_id")) & 
        (F.col("s.id_tienda") == F.col("v.id_tienda")) & 
        (F.col("s.fec_snapshot") == F.col("v.fec_trans")),
        "left"
    ) \
    .select(
        F.col("s.id_snapshot").alias("id_inventario"),
        F.date_format("s.fec_snapshot", "yyyyMMdd").cast("integer").alias("sk_tiempo"),
        F.col("s.fec_snapshot"),
        F.col("s.art_id").alias("sk_producto"),
        F.col("s.id_tienda").alias("sk_tienda"),
        F.col("s.stock_fisico"),
        F.col("s.stock_transito"),
        F.col("s.stock_reservado"),
        F.col("s.stock_minimo_config"),
        F.coalesce(F.col("v.promedio_consumo_14dias"), F.lit(0.0)).alias("promedio_consumo_14dias"),
        F.when(F.coalesce(F.col("v.promedio_consumo_14dias"), F.lit(0.0)) > 0, 
               F.round(F.col("s.stock_fisico") / F.col("v.promedio_consumo_14dias"), 2))
         .otherwise(F.lit(999.0)).alias("cobertura_dias"),
        F.when(
            (F.coalesce(F.col("s.stock_fisico") / F.col("v.promedio_consumo_14dias"), F.lit(999.0)) < 7) & 
            (F.coalesce(F.col("v.promedio_consumo_14dias"), F.lit(0.0)) > 0), True
        ).otherwise(False).alias("alerta_quiebre"),
        (F.col("s.stock_fisico") - F.col("s.stock_minimo_config")).alias("diferencia_stock_minimo")
    )

fact_inventario.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("fact_inventario")


# -------------------------------------------------------------------------
# 7. FACT_DEVOLUCIONES (Join Venta Origen y Motivo Legible)
# -------------------------------------------------------------------------
fact_devoluciones = df_devoluciones.alias("d") \
    .join(df_ventas.alias("v"), F.col("d.id_trans_origen") == F.col("v.id_trans"), "left") \
    .join(df_art.alias("a"), F.col("d.art_id") == F.col("a.art_id"), "left") \
    .select(
        F.col("d.id_devolucion"),
        F.col("d.id_trans_origen"),
        F.col("d.fec_devolucion"),
        F.col("d.art_id").alias("sk_producto"),
        F.col("d.id_tienda").alias("sk_tienda"),
        F.col("d.canal_devolucion"),
        F.col("d.qty_devuelta"),
        F.col("d.vr_reembolso"),
        F.coalesce(F.col("v.precio_unitario_venta"), F.lit(0.0)).alias("precio_original_venta"),
        F.when(F.col("d.motivo_cod").isin(["DEF", "DEFECTUOSO"]), "Producto Defectuoso")
         .when(F.col("d.motivo_cod").isin(["TAL", "TALLA"]), "Talla / Tamaño Incorrecto")
         .when(F.col("d.motivo_cod").isin(["ERR", "ERROR"]), "Envío Erróneo")
         .otherwise("Otro Motivo").alias("motivo_descripcion"),
        F.col("a.id_categ_n1").alias("categoria_nivel_1")
    )

fact_devoluciones.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("fact_devoluciones")


# -------------------------------------------------------------------------
# 8. FACT_RFM_CLIENTES (Score RFM + Segmentación de Negocio)
# -------------------------------------------------------------------------
fecha_limite_rfm = F.date_sub(F.lit(max_fecha_proceso), 90)

ventas_90d = df_ventas.filter(F.col("fec_trans") >= fecha_limite_rfm)

rfm_base = ventas_90d.groupBy("id_miembro").agg(
    F.datediff(F.lit(max_fecha_proceso), F.max("fec_trans")).alias("recency_dias"),
    F.countDistinct("id_trans").alias("frequency_trans"),
    F.sum("vr_total_trans").alias("monetary_val")
)

w_r = Window.orderBy(F.col("recency_dias").desc())
w_f = Window.orderBy(F.col("frequency_trans").asc())
w_m = Window.orderBy(F.col("monetary_val").asc())

fact_rfm = rfm_base \
    .withColumn("r_score", F.ntile(5).over(w_r)) \
    .withColumn("f_score", F.ntile(5).over(w_f)) \
    .withColumn("m_score", F.ntile(5).over(w_m)) \
    .withColumn("segmento_rfm", F.concat(F.lit("R"), F.col("r_score"), F.lit("-F"), F.col("f_score"), F.lit("-M"), F.col("m_score"))) \
    .withColumn(
        "nombre_segmento",
        F.when((F.col("r_score") >= 4) & (F.col("f_score") >= 4) & (F.col("m_score") >= 4), "Champions / Pro")
         .when((F.col("r_score") >= 3) & (F.col("f_score") >= 3), "Clientes Fieles")
         .when((F.col("r_score") <= 2) & (F.col("f_score") >= 3), "En Riesgo")
         .when((F.col("r_score") == 1) & (F.col("f_score") <= 2), "Abandono")
         .otherwise("En Desarrollo / Ocasionales")
    ) \
    .join(df_crm.select("id_miembro", "id_miembro_hash"), "id_miembro", "inner") \
    .select(
        F.col("id_miembro_hash").alias("sk_cliente"),
        F.col("recency_dias"),
        F.col("frequency_trans"),
        F.col("monetary_val"),
        F.col("r_score"),
        F.col("f_score"),
        F.col("m_score"),
        F.col("segmento_rfm"),
        F.col("nombre_segmento"),
        F.lit(max_fecha_proceso).alias("fec_calculo_rfm")
    )

fact_rfm.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("fact_rfm_clientes")

print(f"¡Modelo Dimensional Capa Gold creado exitosamente! ID: {execution_id}")
