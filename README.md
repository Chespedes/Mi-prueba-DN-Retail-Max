# Prueba Técnica Data Engineering 

## Declaración de Escenario y Plataforma
* **Escenario Seleccionado:** Escenario B - RetailMax
* **Plataforma Implementada:** Microsoft Fabric (Lakehouse architecture & Data Factory Orchestration)
* **Lenguaje Principal:** PySpark / Spark SQL

---

## Arquitectura de Solución (Medallion Architecture)

El flujo de procesamiento sigue la arquitectura Medallion tal cual fue solicitada y  para garantizar calidad, trazabilidad y rendimiento en el Data Lakehouse:

1. **Bronze (Data Cruda):** Ingesta directa de las tablas transaccionales desde el origen SQL Server mediante pipeline sin transformaciones.
2. **Silver (Zona de limpieza):** Limpieza de datos, estandarización de tipos, deduplicación, manejo de valores nulos y generación de llaves subrogadas (`sk_cliente`, `sk_producto`).
3. **Gold (Zona final):** Modelado multidimensional enfocado en el análisis RFM (Recency, Frequency, Monetary) para segmentación de clientes y cálculo de alertas de negocio.

---

## Orquestación del Pipeline

La orquestación de extremo a extremo se realiza mediante Microsoft Fabric Data Factory a través del pipeline `PL_retailmax_medallion`:

* **Actividad 1 (`Invoke Pipeline`):** Desencadena el pipeline `Ext_Sqlsrvr_Bronze` para la ingesta del origen.
* **Actividad 2 (`Notebook`):** Procesa las reglas de transformación hacia la capa Silver (`02_Silver_processing`).
* **Actividad 3 (`Notebook`):** Genera la capa analítica Gold (`03_Gold_processing`) con las métricas RFM.

> *La evidencia gráfica del pipeline ejecutado con éxito se encuentra documentada en la carpeta `/docs`.*

---

## Modelo RFM y Reglas de Negocio
* **Recency:** Días transcurridos desde la última transacción del cliente.
* **Frequency:** Cantidad total de transacciones completadas por el cliente.
* **Monetary (`monetary_val`):** Monto acumulado total de ventas generadas.
