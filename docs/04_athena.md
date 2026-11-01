# Analítica con Amazon Athena

## Objetivo y alcance

Consultar los datos mensuales de precipitaciones procesados por AWS Glue y registrados en Glue Data Catalog.

## Configuración utilizada

- Región: `us-east-1`.
- Usuario de ejecución: `de_noaa` (IAM); permisos de consulta mediante la política `de_noaa_athena`, además de permisos ya existentes de Glue y S3.
- Workgroup: `noaa-ghcnd-wg`, en modo SQL bajo demanda.
- Motor: Athena SQL, actualización automática del motor.
- Límite de datos escaneados: **100 MB por consulta**.
- Resultados de consultas: `s3://de-noaa-athena-dblanco16/results/` (almacenamiento administrado por el cliente, cifrado SSE-S3 y configuración del workgroup aplicada obligatoriamente).
- Catálogo: `AwsDataCatalog`; base de datos: `noaa_ghcnd`.

El workgroup no requiere capacidad aprovisionada. Se mantuvieron desactivadas las métricas adicionales de publicación en CloudWatch, los buckets *Requester Pays* y las alertas adicionales por uso del workgroup; la cuenta ya dispone de AWS Budgets. Para consultar se requiere que los archivos Parquet estén presentes en S3 PROCESSED y las particiones estén registradas en Glue Data Catalog.

## Consulta analítica implementada

Archivo: [`../sql/precipitaciones_mensuales.sql`](../sql/precipitaciones_mensuales.sql).

La consulta devuelve una fila por estación, año y mes con precipitación acumulada (`mm`), días observados, días del mes y porcentaje de cobertura. Filtra `pais = 'NZ'` y `anio BETWEEN 2016 AND 2025`, aprovechando las particiones existentes. No suma precipitaciones entre estaciones: cada estación representa una ubicación distinta.

## Evidencia de ejecución

- Estado: **Completado**.
- Registros devueltos: **1.345**.
- Estaciones diferentes: **13** (calculadas a partir del CSV exportado de Athena).
- Datos analizados informados por Athena: **16,36 KB**.
- Tiempo de ejecución mostrado por Athena: **696 ms**.
- Período consultado: **2016–2025**.

## Interpretación y limitaciones

Del CSV descargado de Athena se verificó que el **67,7 %** de los registros mensuales tiene cobertura completa (`cobertura_pct = 100`). Entre 2016 y 2024, la cobertura promedio de los meses presentes en el resultado supera el 90 % en cada año. En **2025**, hay **67 registros mensuales**, frente a **144** en varios años anteriores, y la cobertura media de esos 67 registros es **58,51 %**.

**Advertencia:** el promedio de cobertura se calcula solo sobre filas mensuales existentes; no representa la cobertura de los meses ausentes. Asimismo, `prcp_acumulada_mm` es la suma de las precipitaciones de los días observados, sin imputación de faltantes. Por lo tanto, no corresponde comparar acumulados entre períodos con cobertura muy distinta ni interpretar la suma de estaciones como precipitación nacional.

## Posibles ampliaciones

Comparación entre estaciones, patrones estacionales y análisis de anomalías. La selección de datos para entrenamiento y evaluación predictiva se desarrollará en la etapa de Machine Learning.
