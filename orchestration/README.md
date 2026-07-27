## 📊 Estado de Ejecución y Progreso de Notebooks

A continuación se detalla el progreso, estado y evidencia de la ejecución secuencial de cada componente y notebook dentro del pipeline de Microsoft Fabric para el caso *RetailMax*:

| Capa / Fase | Nombre del Notebook / Archivo | Descripción de la Tarea | Estado | Evidencia / Observaciones |
| :--- | :--- | :--- | :--- | :--- |
| **0. Setup** | `01_generate_master_data.py` y `02_generate_transactional_data.py` | Generación y carga del modelo transaccional de origen (Dummy Data) solicitado por la prueba. | ✅ **Completado** | ![Conteo Datos](docs/Evidencia_poblado_tablas.png) *(1,000,000 reg. en ventas)* |
| **1. Bronze** | `01_Bronze_Ingestion.ipynb` | Ingesta *Raw* desde las tablas SQL hacia el Lakehouse en formato Parquet. | ✅ **Completado** | Validación de volumetría y estructura OK. |
| **2. Silver** | `02_Silver_Transformation.ipynb`| Limpieza, tipado, estandarización y validación de nulos/duplicados. | ✅ **Completado** | Calidad de datos aplicada por dominio. |
| **3. Gold** | `03_Gold_Aggregations.ipynb` | Modelado dimensional (Star Schema) y cálculo de métricas de negocio. | ✅ **Completado** | Tablas de hechos y dimensiones listas para consumo. |
| **4. Monitoreo** | `04_Pipeline_Orchestration.ipynb` | Orquestación end-to-end, manejo de reintentos y alertas de control. | ✅ **Completado** | Logs de éxito/fallo configurados. |
