# Changelog

Todas las modificaciones notables de este proyecto serán documentadas en este archivo.

El formato está basado en [Keep a Changelog](https://keepachangelog.com/es-ES/1.0.0/),
y este proyecto adhiere a [Semantic Versioning](https://semver.org/spec/v2.0.0-2.html).

## [1.0.0] - 2026-07-26
### Añadido
- Creación de la estructura base de infraestructura en la carpeta `/infra` (Terraform conceptual).
- Implementación del pipeline de ingesta masiva en Fabric (`Ext_Sqlsrvr_Bronze`) para la capa Bronze utilizando control de cargas dinámico vía Lookup y ForEach.
- Scripts DDL iniciales para la creación de las tablas transaccionales y maestras de origen en la carpeta `/sql`.
- Configuración de scripts de transformación para las capas Silver y Gold.
- Documentación del linaje de datos y catálogo básico en la carpeta `/docs`.
- Definición de políticas de seguridad, control de acceso por roles (Ingenieria, Analista, Admin) y enmascaramiento de datos sensibles.

