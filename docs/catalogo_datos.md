# Catálogo de Datos - RetailMax Analytics

Este documento detalla las principales entidades y campos calculados gestionados en el pipeline analítico de Microsoft Fabric.

##  Entidades Principales

### 1. TRANS_VENTAS (Capa Bronze / Silver)
Tabla transaccional que almacena el detalle de las ventas realizadas en los canales físicos y digitales.
* **id_trans** (`BIGINT`): Identificador único de la transacción.
* **fec_trans** (`DATE`): Fecha en la que se efectuó la transacción.
* **qty_vendida** (`INT`): Cantidad de unidades vendidas del artículo.
* **precio_unitario_venta** (`DECIMAL`): Precio unitario aplicado al momento de la venta.

### 2. MSTR_ARTICULOS (Capa Silver / Gold)
Catálogo maestro de productos de la compañía.
* **art_id** (`INT`): Llave primaria del artículo.
* **desc_art** (`VARCHAR`): Descripción comercial del producto.
* **precio_lista** (`DECIMAL`): Precio de lista oficial de referencia.

---

##  Linaje de Datos y Campos Calculados (Capa Gold)

Documentación del linaje para tres indicadores clave de negocio calculados en la capa Gold:

1. **Margen de Utilidad Bruta (`margen_bruto_pct`)**
   * **Tabla de Origen:** `TRANS_VENTAS` y `MSTR_ARTICULOS`.
   * **Transformaciones Aplicadas:** Se realiza un `JOIN` entre ventas y artículos, calculando la diferencia entre el precio de venta real y el costo de referencia, dividido por el precio de venta.
   * **Propósito de Negocio:** Evaluar la rentabilidad porcentual por línea de producto y categoría en tiempo real.

2. **Valor Total de Descuento Otorgado (`total_descuentos_monto`)**
   * **Tabla de Origen:** `TRANS_VENTAS`.
   * **Transformaciones Aplicadas:** Agrupación por canal de venta (`canal_venta`) y aplicación de una función de suma sobre el campo `descuento_aplicado`.
   * **Propósito de Negocio:** Monitorear el impacto financiero de las campañas promocionales y descuentos en las tiendas.

3. **Rotación de Inventario y Cobertura (`cobertura_dias_stock`)**
   * **Tabla de Origen:** `INV_STOCK_DIARIO` y `TRANS_VENTAS`.
   * **Transformaciones Aplicadas:** Cálculo del promedio de stock físico dividido entre la tasa de venta diaria de los últimos 30 días.
   * **Propósito de Negocio:** Identificar riesgos de quiebre de stock o sobrestock en las tiendas físicas para optimizar las reordenes de proveedores.
