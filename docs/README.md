# Arquitectura de Datos y Modelo Entidad-Relación (MER)

## 1. Modelo Origen Transaccional (Capa Bronze - 7 Tablas)
Estructura normalizada ingestada desde el origen SQL Server hacia la capa Bronze:

```mermaid
erDiagram
    CLIENTES ||--o{ VENTAS : realiza
    TIENDAS ||--o{ VENTAS : procesa
    PRODUCTOS ||--o{ DETALLE_VENTAS : contiene
    VENTAS ||--o{ DETALLE_VENTAS : compone
    CATEGORIAS ||--o{ PRODUCTOS : clasifica
    CLIENTES ||--o| METRICAS_RFM : calcula
    CLIENTES ||--o{ ALERTAS : genera

    CLIENTES {
        string id_cliente PK
        string nombre_cliente
        string email
        string categoria_cliente
    }
    TIENDAS {
        string id_tienda PK
        string nombre_tienda
        string ciudad
        string region
    }
    PRODUCTOS {
        string id_producto PK
        string id_categoria FK
        string nombre_producto
        decimal precio_unitario
    }
    CATEGORIAS {
        string id_categoria PK
        string nombre_categoria
    }
    VENTAS {
        string id_venta PK
        string id_cliente FK
        string id_tienda FK
        timestamp fecha_venta
    }
    DETALLE_VENTAS {
        string id_detalle PK
        string id_venta FK
        string id_producto FK
        int cantidad
        decimal precio_total
    }
    ALERTAS {
        string id_alerta PK
        string id_cliente FK
        string tipo_alerta
        timestamp fecha_alerta
    }
```

---

## 2. Modelo Dimensional y Métricas (Capa Gold - Star Schema)
Transformación y desnormalización hacia el modelo analítico para consumo y análisis RFM:

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
    }
    DIM_PRODUCTO {
        string sk_producto PK
        string id_producto_origen
        string nombre_producto
        string categoria
    }
    DIM_TIENDA {
        string sk_tienda PK
        string id_tienda_origen
        string nombre_tienda
    }
    FACT_VENTAS {
        string id_transaccion PK
        string sk_cliente FK
        string sk_producto FK
        string sk_tienda FK
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
