# Modelo Entidad-Relación (MER) - RetailMax

## Modelo Dimensional (Capa Gold)

El siguiente diagrama ilustra el modelo en estrella (Star Schema) diseñado para soportar el análisis de ventas y el modelo de segmentación RFM:

```mermaid
erDiagram
    DIM_CLIENTE ||--o{ FACT_VENTAS : realiza
    DIM_PRODUCTO ||--o{ FACT_VENTAS : contiene
    DIM_TIENDA ||--o{ FACT_VENTAS : procesa
    DIM_CLIENTE ||--o| METRICAS_RFM : calcula

    DIM_CLIENTE {
        string sk_cliente PK
        string id_cliente_origen
        string nombre_cliente
        string email
        string categoria_cliente
    }

    DIM_PRODUCTO {
        string sk_producto PK
        string id_producto_origen
        string nombre_producto
        string categoria
        decimal precio_unitario
    }

    FACT_VENTAS {
        string id_transaccion PK
        string sk_cliente FK
        string sk_producto FK
        timestamp fecha_transaccion
        int cantidad
        decimal monto_total
    }

    METRICAS_RFM {
        string sk_cliente PK, FK
        int recency_dias
        int frequency_compras
        decimal monetary_monto
        string segmento_cliente
        boolean alerta_churn
    }
```

### Descripción de Entidades Principales:
* **`FACT_VENTAS`:** Tabla de hechos granular a nivel de ítem/transacción vendida.
* **`DIM_CLIENTE` / `DIM_PRODUCTO`:** Dimensiones con llaves subrogadas (`sk_`) generadas en la capa Silver para abstraer cambios en los IDs de origen.
* **`METRICAS_RFM`:** Tabla analítica en capa Gold con métricas agregadas por cliente, puntuación RFM (1-5) y banderas de alertas de negocio (`alerta_churn`).
