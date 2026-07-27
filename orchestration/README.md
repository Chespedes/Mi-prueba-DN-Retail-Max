Capa / Fase,Nombre del Notebook / Script,Descripción de la Tarea,Estado,Evidencia / Observaciones
0. Setup,00_Data_Generator.ipynb,Generación y carga del modelo transaccional de origen (Dummy Data).,✅ Completado,"1,000,000 registros en TRANS_VENTAS."
1. Bronze,01_Bronze_Ingestion.ipynb,Ingesta Raw desde las tablas SQL hacia el Lakehouse en formato Parquet.,✅ Completado,Carga incremental y control de volumen OK.
2. Silver,02_Silver_Transformation.ipynb,"Limpieza, tipado, estandarización y validación de nulos/duplicados.",✅ Completado,Calidad de datos aplicada por dominio.
3. Gold,03_Gold_Aggregations.ipynb,Modelado dimensional (Star Schema) y cálculo de métricas de negocio.,✅ Completado,Tablas de hechos y dimensiones listas para consumo.
4. Monitoreo,04_Pipeline_Orchestration.ipynb,"Orquestación end-to-end, manejo de reintentos y alertas de control.",✅ Completado,Logs de éxito/fallo configurados.#
