# Procesamiento ETL con AWS Glue

## 1. Objetivo

Transformar los registros meteorológicos NOAA GHCN-D almacenados en S3 RAW en datasets Parquet para análisis históricos y futuras consultas con Amazon Athena y aplicaciones de Machine Learning.

**Flujo:** S3 RAW → AWS Glue ETL → S3 PROCESSED.

Se generan dos datasets:

- **Diario:** observaciones meteorológicas por estación, fecha y variable.
- **Mensual:** precipitación acumulada por estación y mes, acompañada de indicadores de cobertura.

## 2. Diseño del ETL

El procesamiento se implementó en `scripts/noaa_etl.py`, con las siguientes características:

- AWS Glue Python Shell, Python 3.9, capacidad inicial de 1/16 DPU (0,0625).
- Procesamiento batch, manual y bajo demanda.
- Lectura de CSV por estación para limitar el consumo de memoria.
- Parámetros configurables: país, fecha inicial, fecha final, variables y buckets de origen y destino.
- Reprocesamiento completo, sin ejecución incremental.
- Generación de Parquet con compresión Snappy, consolidado por país y año.

**Transformaciones:**

- Conversión de fechas y valores numéricos.
- Conversión de `PRCP`, `TMAX`, `TMIN` y `TAVG` dividiendo los valores originales por 10.
- `SNOW` conserva su unidad original en milímetros.
- Exclusión de registros con valores faltantes, no numéricos o `Q_FLAG` informado.
- Conservación de precipitaciones iguales a cero.
- Conservación de las observaciones originales en RAW.

Los registros descartados se contabilizan durante el procesamiento.

- [Script ](scripts/noaa_etl.py)

## 3. Datasets de salida

**S3 PROCESSED:** `de-noaa-processed-dblanco16`

**Observaciones diarias**

Ubicación: `ghcnd/diario/pais=NZ/anio=AAAA/datos.parquet`

Campos: `STATION`, `DATE`, `ELEMENT`, `valor_original`, `valor`, `M_FLAG`, `S_FLAG`, `OBS_TIME`.

**Precipitaciones mensuales**

Ubicación: `ghcnd/mensual/pais=NZ/anio=AAAA/datos.parquet`

Campos: `STATION`, `mes`, `dias_mes`, `dias_observados`, `cobertura_pct`, `prcp_acumulada_mm`.

El acumulado mensual corresponde a la suma de precipitaciones válidas disponibles. No se imputan valores para días sin observaciones. El porcentaje de cobertura permite evaluar la representatividad del acumulado.

Ambos datasets están particionados por país y año, sin generar particiones individuales por estación.

## 4. Configuración AWS

| Configuración | Valor |
|---|---|
| Región | `us-east-1` |
| Servicio | AWS Glue |
| Job | `noaa-ghcnd-etl` |
| Tipo | Python Shell |
| Capacidad | 0,0625 DPU |
| Rol IAM | `de_noaa_glue_role` |
| Política administrada | `AWSGlueServiceRole` |
| Política S3 | `de_noaa_glue_s3` |
| Ruta prevista del script | `s3://de-noaa-processed-dblanco16/scripts/noaa_etl.py` |
| Ejecución | Manual, sin programación automática |

El rol dispone de permisos específicos para leer RAW y escribir en PROCESSED.

La configuración puede permanecer creada sin ejecuciones activas. El script debe estar disponible en S3 antes de ejecutar el job.

### Selección del tipo de job

Se seleccionó **AWS Glue Python Shell** en lugar de un job basado en Apache Spark, considerando el volumen de datos, la naturaleza de las transformaciones y la frecuencia de ejecución.

El procesamiento se realiza por estación meteorológica, mediante Python, sin requerir procesamiento distribuido. Las operaciones comprenden limpieza, conversión de tipos y unidades, agregaciones temporales y generación de archivos Parquet.

Se configuró una capacidad de **0,0625 DPU**, con el objetivo de minimizar el costo de ejecución y utilizar recursos acordes con el volumen inicial.

Esta elección simplifica la implementación y permite reutilizar el código Python validado localmente. Su escalabilidad está condicionada por la memoria y capacidad de procesamiento de una ejecución no distribuida.

Si el volumen de datos creciera significativamente, correspondería evaluar una arquitectura de procesamiento distribuido mediante AWS Glue Spark.