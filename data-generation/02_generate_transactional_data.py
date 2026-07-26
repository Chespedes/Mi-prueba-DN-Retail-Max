import random
from datetime import datetime, timedelta
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# 1. Configuration & Seed Initialization
SEED = 42
random.seed(SEED)
np.random.seed(SEED)

# Connection settings (Microsoft Fabric SQL Database)
access_token = mssparkutils.credentials.getToken("https://database.windows.net/")
server_name = "<FABRIC_SQL_SERVER_ENDPOINT>"
database_name = "<FABRIC_DATABASE_NAME>"
jdbc_url = f"jdbc:sqlserver://{server_name}:1433;database={database_name};encrypt=true;trustServerCertificate=false;"

# Volume limits & Foreign Key boundaries
num_transacciones = 1000000
num_stock = 750000
num_devoluciones = 50000

max_articulos = 5000
max_tiendas = 150
max_miembros = 50000

fecha_fin = datetime.now()
fecha_inicio = fecha_fin - timedelta(days=365)
dias_rango = (fecha_fin - fecha_inicio).days

# 2. Transactional Data Generation (NumPy Vectorized)

# A. TRANS_VENTAS (1,000,000 records)
print("Generando dataset: TRANS_VENTAS...")

fec_offset = np.random.randint(0, dias_rango, size=num_transacciones)
fechas_trans = [
    (fecha_inicio + timedelta(days=int(d))).strftime("%Y-%m-%d")
    for d in fec_offset
]

horas_trans = [
    f"{h:02d}:{m:02d}:{s:02d}"
    for h, m, s in zip(
        np.random.randint(8, 22, size=num_transacciones),
        np.random.randint(0, 60, size=num_transacciones),
        np.random.randint(0, 60, size=num_transacciones),
    )
]

# Simulate ~5% null values for id_miembro to emulate guest checkout / unlinked sales
miembros_rand = np.random.randint(1, max_miembros + 1, size=num_transacciones)
mask_nulls = np.random.rand(num_transacciones) < 0.05
miembros_data = [
    None if is_null else int(m)
    for m, is_null in zip(miembros_rand, mask_nulls)
]

trans_ventas_data = list(
    zip(
        range(1, num_transacciones + 1),
        miembros_data,
        np.random.randint(1, max_tiendas + 1, size=num_transacciones).tolist(),
        np.random.randint(1, max_articulos + 1, size=num_transacciones).tolist(),
        fechas_trans,
        horas_trans,
        np.random.randint(1, 10, size=num_transacciones).tolist(),
        np.round(np.random.uniform(5.0, 500.0, size=num_transacciones), 2).tolist(),
        np.round(np.random.uniform(0.0, 50.0, size=num_transacciones), 2).tolist(),
        np.random.choice(
            ["Efectivo", "Tarjeta Credito", "Tarjeta Debito", "PSE"],
            size=num_transacciones,
        ).tolist(),
        np.random.choice(
            ["Fisico", "Online", "App"], size=num_transacciones
        ).tolist(),
    )
)

df_trans = spark.createDataFrame(
    trans_ventas_data,
    [
        "id_trans",
        "id_miembro",
        "id_tienda",
        "art_id",
        "fec_trans",
        "hra_trans",
        "qty_vendida",
        "precio_unitario_venta",
        "descuento_aplicado",
        "tipo_pago",
        "canal_venta",
    ],
)

# B. INV_STOCK_DIARIO (750,000 records)
print("Generando dataset: INV_STOCK_DIARIO...")

fec_offset_stock = np.random.randint(0, dias_rango, size=num_stock)
fechas_stock = [
    (fecha_inicio + timedelta(days=int(d))).strftime("%Y-%m-%d")
    for d in fec_offset_stock
]

stock_data = list(
    zip(
        range(1, num_stock + 1),
        np.random.randint(1, max_articulos + 1, size=num_stock).tolist(),
        np.random.randint(1, max_tiendas + 1, size=num_stock).tolist(),
        fechas_stock,
        np.random.randint(0, 500, size=num_stock).tolist(),
        np.random.randint(0, 100, size=num_stock).tolist(),
        np.random.randint(0, 50, size=num_stock).tolist(),
        np.random.randint(10, 50, size=num_stock).tolist(),
        np.random.randint(100, 600, size=num_stock).tolist(),
    )
)

df_stock = spark.createDataFrame(
    stock_data,
    [
        "id_snapshot",
        "art_id",
        "id_tienda",
        "fec_snapshot",
        "stock_fisico",
        "stock_transito",
        "stock_reservado",
        "stock_minimo_config",
        "stock_maximo_config",
    ],
)

# C. POST_DEVOLUCIONES (50,000 records)
print("Generando dataset: POST_DEVOLUCIONES...")

fec_offset_dev = np.random.randint(0, dias_rango, size=num_devoluciones)
fechas_dev = [
    (fecha_inicio + timedelta(days=int(d))).strftime("%Y-%m-%d")
    for d in fec_offset_dev
]

dev_data = list(
    zip(
        range(1, num_devoluciones + 1),
        np.random.randint(1, num_transacciones + 1, size=num_devoluciones).tolist(),
        np.random.randint(1, max_articulos + 1, size=num_devoluciones).tolist(),
        np.random.randint(1, max_tiendas + 1, size=num_devoluciones).tolist(),
        fechas_dev,
        np.random.randint(1, 3, size=num_devoluciones).tolist(),
        np.random.choice(
            ["DEFECTO", "TALLA_INCORRECTA", "ARREPENTIMIENTO", "OTRO"],
            size=num_devoluciones,
        ).tolist(),
        np.random.choice(
            ["Tienda", "Correo", "Casillero"], size=num_devoluciones
        ).tolist(),
        np.random.choice(
            ["APROBADO", "PENDIENTE", "RECHAZADO"], size=num_devoluciones
        ).tolist(),
        np.round(np.random.uniform(5.0, 300.0, size=num_devoluciones), 2).tolist(),
    )
)

df_dev = spark.createDataFrame(
    dev_data,
    [
        "id_devolucion",
        "id_trans_origen",
        "art_id",
        "id_tienda",
        "fec_devolucion",
        "qty_devuelta",
        "motivo_cod",
        "canal_devolucion",
        "estado_devolucion",
        "vr_reembolso",
    ],
)

# 3. High-Volume Batch JDBC Ingestion

tablas_grandes = {
    "TRANS_VENTAS": df_trans,
    "INV_STOCK_DIARIO": df_stock,
    "POST_DEVOLUCIONES": df_dev,
}

for nombre_tabla, df in tablas_grandes.items():
    print(f"Escribiendo lote JDBC para {nombre_tabla}...")

    df.write.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", nombre_tabla) \
        .option("accessToken", access_token) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .option("batchsize", "20000") \
        .mode("append") \
        .save()

print("Carga transaccional masiva finalizada con éxito.")
