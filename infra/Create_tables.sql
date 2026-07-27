-- =====================================================================
-- Esquema Base de Datos Transaccional - RetailMax  type SQL DATABASE (Data de negocio)
-- =====================================================================

CREATE TABLE MSTR_PROVEEDORES (
    id_proveedor INT PRIMARY KEY,
    razon_social VARCHAR(150),
    pais_origen VARCHAR(50),
    tiempo_repo_dias INT,
    calificacion_calidad FLOAT,
    activo INT
);

CREATE TABLE MSTR_ARTICULOS (
    art_id INT PRIMARY KEY,
    cod_barra VARCHAR(50),
    desc_art VARCHAR(200),
    id_categ_n1 INT,
    id_categ_n2 INT,
    id_categ_n3 INT,
    id_proveedor INT,
    precio_lista DECIMAL(12,2),
    peso_kg DECIMAL(8,2),
    unid_medida VARCHAR(20),
    activo INT,
    fec_alta DATE
);

CREATE TABLE MSTR_TIENDAS (
    id_tienda INT PRIMARY KEY,
    nom_tienda VARCHAR(100),
    tipo_tienda VARCHAR(50),
    id_ciudad VARCHAR(50),
    id_pais VARCHAR(50),
    metros_cuadrados INT,
    activo INT,
    fec_apertura DATE
);

CREATE TABLE CRM_MIEMBROS (
    id_miembro INT PRIMARY KEY,
    fec_registro DATE,
    id_ciudad VARCHAR(50),
    genero VARCHAR(20),
    rango_edad VARCHAR(20),
    canal_pref VARCHAR(50),
    activo INT,
    fec_ultima_compra DATE
);

CREATE TABLE TRANS_VENTAS (
    id_trans BIGINT,
    id_miembro INT,
    id_tienda INT,
    art_id INT,
    fec_trans DATE,
    hra_trans VARCHAR(8),
    qty_vendida INT,
    precio_unitario_venta DECIMAL(12,2),
    descuento_aplicado DECIMAL(12,2),
    tipo_pago VARCHAR(50),
    canal_venta VARCHAR(50)
);

CREATE TABLE INV_STOCK_DIARIO (
    id_snapshot BIGINT,
    art_id INT,
    id_tienda INT,
    fec_snapshot DATE,
    stock_fisico INT,
    stock_transito INT,
    stock_reservado INT,
    stock_minimo_config INT,
    stock_maximo_config INT
);

CREATE TABLE POST_DEVOLUCIONES (
    id_devolucion BIGINT,
    id_trans_origen BIGINT,
    art_id INT,
    id_tienda INT,
    fec_devolucion DATE,
    qty_devuelta INT,
    motivo_cod VARCHAR(50),
    canal_devolucion VARCHAR(50),
    estado_devolucion VARCHAR(50),
    vr_reembolso DECIMAL(12,2)
);
