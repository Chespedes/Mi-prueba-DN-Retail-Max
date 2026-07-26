# 📐 Modelo Dimensional (Capa Gold - Star Schema / Constellation)

El modelo de la capa Gold consolida los procesos clave del negocio RetailMax: **Ventas**, **Devoluciones**, **Control de Inventarios** y **Segmentación de Clientes (RFM)**.

```mermaid
erDiagram
    %% Relaciones Fact Ventas
    DIM_CLIENTE ||--o{ FACT_VENTAS : realiza
    DIM_PRODUCTO ||--o{ FACT_VENTAS : contiene
    DIM_TIENDA ||--o{ FACT_VENTAS : procesa
    DIM_TIEMPO ||--o{ FACT_VENTAS : ocurre_en

    %% Relaciones Fact Devoluciones
    DIM_CLIENTE ||--o{ FACT_DEVOLUCIONES : solicita
    DIM_PRODUCTO ||--o{ FACT_DEVOLUCIONES : involucra
    DIM_TIENDA ||--o{ FACT_DEVOLUCIONES : registra
    DIM_TIEMPO ||--o{ FACT_DEVOLUCIONES : ocurre_en

    %% Relaciones Fact Inventario
    DIM_PRODUCTO ||--o{ FACT_INVENTARIO : registrado_en
    DIM_TIENDA ||--o{ FACT_INVENTARIO : ubicado_en
    DIM_TIEMPO ||--o{ FACT_INVENTARIO : medido_en

    %% Relación Métricas RFM
    DIM_CLIENTE ||--o| METRICAS_RFM : evalua

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
        string region
    }

    DIM_TIEMPO {
        int sk_fecha PK
        date fecha
        int anio
        int mes
        string dia_semana
    }

    FACT_VENTAS {
        string id_transaccion PK
        string sk_cliente FK
        string sk_producto FK
        string sk_tienda FK
        int sk_fecha FK
        int cantidad
        decimal monto_total
    }

    FACT_DEVOLUCIONES {
        string id_devolucion PK
        string id_transaccion_origen
        string sk_cliente FK
        string sk_producto FK
        string sk_tienda FK
        int sk_fecha FK
        int cantidad_devuelta
        decimal monto_reembolsado
        string motivo_devolucion
    }

    FACT_INVENTARIO {
        string sk_inventario PK
        string sk_producto FK
        string sk_tienda FK
        int sk_fecha FK
        int stock_disponible
        int reorder_point
        boolean alerta_stock_bajo
    }

    METRICAS_RFM {
        string sk_cliente PK, FK
        int recency_dias
        int frequency_compras
        decimal monetary_monto
        string segmento_cliente
        boolean alerta_churn
    }
