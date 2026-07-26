import random
from datetime import datetime, timedelta
import numpy as np
from pyspark.sql import SparkSession

# Configuration & Seed initialization
SEED = 42
random.seed(SEED)
np.random.seed(SEED)

fecha_fin = datetime.now()
fecha_inicio = fecha_fin - timedelta(days=365)

volumenes = {
    "MSTR_PROVEEDORES": 800,
    "MSTR_ARTICULOS": 5000,
    "MSTR_TIENDAS": 150,
    "CRM_MIEMBROS": 50000
}

# 1. Master Data Generation

# A. MSTR_PROVEEDORES
paises = ["Colombia", "Mexico", "Chile", "Peru", "Ecuador"]
prov_data = [(
    i, 
    f"Proveedor_{i} S.A.", 
    random.choice(paises), 
    random.randint(1, 15), 
    round(random.uniform(3.0, 5.0), 2), 
    1
) for i in range(1, volumenes["MSTR_PROVEEDORES"] + 1)]

df_prov = spark.createDataFrame(
    prov_data, 
    ["id_proveedor", "razon_social", "pais_origen", "tiempo_repo_dias", "calificacion_calidad", "activo"]
)

# B. MSTR_ARTICULOS
unidades = ["UN", "KG", "LITRO", "CAJA"]
art_data = [(
    i, 
    f"770{i:09d}", 
    f"Producto Referencia {i}", 
    random.randint(1, 6), 
    random.randint(10, 30), 
    random.randint(100, 500), 
    random.randint(1, volumenes["MSTR_PROVEEDORES"]), 
    round(random.uniform(5.0, 500.0), 2), 
    round(random.uniform(0.1, 20.0), 2), 
    random.choice(unidades), 
    1, 
    (fecha_inicio + timedelta(days=random.randint(0, 180))).date()
) for i in range(1, volumenes["MSTR_ARTICULOS"] + 1)]

df_art = spark.createDataFrame(
    art_data, 
    ["art_id", "cod_barra", "desc_art", "id_categ_n1", "id_categ_n2", "id_categ_n3", "id_proveedor", "precio_lista", "peso_kg", "unid_medida", "activo", "fec_alta"]
)

# C. MSTR_TIENDAS
tipos_tienda = ["Hipermercado", "Supermercado", "Conveniencia"]
ciudades = ["Bogota", "CDMX", "Santiago", "Lima", "Quito"]
tiendas_data = [(
    i, 
    f"Tienda Express {i}", 
    random.choice(tipos_tienda), 
    random.choice(ciudades), 
    random.choice(paises), 
    random.randint(100, 3500), 
    1, 
    (fecha_inicio - timedelta(days=random.randint(300, 2000))).date()
) for i in range(1, volumenes["MSTR_TIENDAS"] + 1)]

df_tiendas = spark.createDataFrame(
    tiendas_data, 
    ["id_tienda", "nom_tienda", "tipo_tienda", "id_ciudad", "id_pais", "metros_cuadrados", "activo", "fec_apertura"]
)

# D. CRM_MIEMBROS
generos = ["M", "F", None]
rangos_edad = ["18-25", "26-35", "36-50", "51+", None]
canales = ["App", "Web", "Tienda"]

crm_data = []
for i in range(1, volumenes["CRM_MIEMBROS"] + 1):
    f_reg = fecha_inicio + timedelta(days=random.randint(0, 200))
    f_ult = f_reg + timedelta(days=random.randint(0, 150))
    crm_data.append((
        i, 
        f_reg.date(), 
        random.choice(ciudades), 
        random.choice(generos), 
        random.choice(rangos_edad), 
        random.choice(canales), 
        1, 
        f_ult.date()
    ))

df_crm = spark.createDataFrame(
    crm_data, 
    ["id_miembro", "fec_registro", "id_ciudad", "genero", "rango_edad", "canal_pref", "activo", "fec_ultima_compra"]
)

# 2. Database Ingestion via JDBC (Microsoft Fabric SQL Database)

access_token = mssparkutils.credentials.getToken("https://database.windows.net/")

# Environment Connection Settings
server_name = "<FABRIC_SQL_SERVER_ENDPOINT>" # e.g. xxx.database.fabric.microsoft.com
database_name = "<FABRIC_DATABASE_NAME>"

jdbc_url = f"jdbc:sqlserver://{server_name}:1433;database={database_name};encrypt=true;trustServerCertificate=false;"

tablas = {
    "MSTR_PROVEEDORES": df_prov,
    "MSTR_ARTICULOS": df_art,
    "MSTR_TIENDAS": df_tiendas,
    "CRM_MIEMBROS": df_crm
}

for nombre_tabla, df in tablas.items():
    print(f"Persistiendo lote en tabla: {nombre_tabla}...")
    
    df.write \
      .format("jdbc") \
      .option("url", jdbc_url) \
      .option("dbtable", nombre_tabla) \
      .option("accessToken", access_token) \
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
      .mode("append") \
      .save()

print("Proceso de generación e ingesta inicial finalizado correctamente.")
