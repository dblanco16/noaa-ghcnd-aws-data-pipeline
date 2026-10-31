# Catalogación de datos con AWS Glue Data Catalog

## 1. Objetivo y arquitectura

Registrar los dos datasets Parquet generados por AWS Glue ETL en S3 PROCESSED para su posterior consulta mediante Amazon Athena, sin copiar los archivos al catálogo.

Flujo: `S3 PROCESSED → AWS Glue Data Catalog → Amazon Athena`.

Glue Data Catalog conserva los metadatos de las tablas (columnas, tipos, formato, ubicación y particiones); los datos permanecen almacenados en Amazon S3.

## 2. Decisiones de diseño

- **Tablas externas definidas manualmente**, porque sus esquemas y ubicaciones son conocidos. No se utilizan Glue Crawlers.
- **Particionamiento por país y año** (`pais/anio`), conservando una tabla por dataset, independientemente de la cantidad de países o años.
- **Sincronización mediante Python y boto3**, separada del ETL: `scripts/noaa_catalog.py` detecta las particiones existentes en S3 y registra las faltantes en Glue Data Catalog.
- La sincronización se ejecuta manualmente **después de un ETL exitoso**; no se incorporan disparadores ni servicios de orquestación.
- Las tablas y sus metadatos permanecen creados entre sesiones, aunque se eliminen temporalmente los objetos de S3 para reducir costos.

## 3. Recursos y esquemas

- Región: `us-east-1`.
- Base de datos: `noaa_ghcnd`.
- Bucket de datos: `de-noaa-processed-dblanco16`.

### Tabla `observaciones_diarias`

Ubicación: `s3://de-noaa-processed-dblanco16/ghcnd/diario/`

| Columna | Tipo Glue |
| --- | --- |
| `STATION` | `string` |
| `DATE` | `date` |
| `ELEMENT` | `string` |
| `valor_original` | `int` |
| `valor` | `double` |
| `M_FLAG` | `string` |
| `S_FLAG` | `string` |
| `OBS_TIME` | `string` |

### Tabla `precipitaciones_mensuales`

Ubicación: `s3://de-noaa-processed-dblanco16/ghcnd/mensual/`

| Columna | Tipo Glue |
| --- | --- |
| `STATION` | `string` |
| `mes` | `int` |
| `dias_mes` | `int` |
| `dias_observados` | `int` |
| `cobertura_pct` | `double` |
| `prcp_acumulada_mm` | `double` |

Ambas tablas utilizan archivos **Parquet** con compresión **Snappy** y dos **claves de partición** (no columnas físicas de los Parquet): `pais string` y `anio int`.

Ejemplo de ruta física: `ghcnd/diario/pais=NZ/anio=2020/datos.parquet`.

## 4. Sincronización de particiones

El script `scripts/noaa_catalog.py` realiza las siguientes operaciones:

1. Identifica las particiones `pais=XX/anio=AAAA` que contienen archivos `.parquet` en cada dataset de S3.
2. Consulta las particiones registradas en las tablas de Glue Data Catalog.
3. Compara las ubicaciones y muestra las particiones faltantes y obsoletas.
4. Con `--apply`, registra las particiones faltantes. Si encuentra ubicaciones inconsistentes, detiene la ejecución para revisión.
5. La eliminación de particiones obsoletas requiere `--prune --apply` y está bloqueada si alguno de los datasets no contiene archivos Parquet. **No usar `--prune` para la limpieza temporal de S3 entre sesiones.**

Desde la raíz del repositorio, con el entorno Python y las credenciales AWS configuradas:

```powershell
# Simular: consulta S3 y Glue sin modificar metadatos
python scripts/noaa_catalog.py

# Aplicar: registrar particiones faltantes
python scripts/noaa_catalog.py --apply

# Verificar: no deben quedar particiones nuevas
python scripts/noaa_catalog.py
```

Por defecto se utiliza el perfil AWS `de_noaa` y la región `us-east-1` (ambos configurables mediante `--profile` y `--region`). El script requiere `boto3`.

**Orden de operación:** ejecutar ingesta RAW, ejecutar y comprobar el éxito del job `noaa-ghcnd-etl`, simular la sincronización y aplicar los cambios. Si los archivos PROCESSED fueron eliminados, se pueden conservar las tablas y las particiones catalogadas hasta el próximo procesamiento; una simulación mostrará esas particiones como obsoletas sin eliminarlas.

## 5. Seguridad y permisos

El usuario IAM `de_noaa` emplea una política personalizada `de_noaa_glue_catalog` para consultar y administrar metadatos de Glue Data Catalog, restringiendo las modificaciones a la base `noaa_ghcnd` y sus tablas. La sincronización local usa las credenciales del perfil `de_noaa`.

Se requieren permisos de lectura de S3 PROCESSED y las acciones Glue pertinentes para leer tablas y particiones (`GetTable`, `GetPartitions`) y registrar particiones (`BatchCreatePartition`). La política también contempla administración de bases, tablas y particiones según lo configurado en IAM.