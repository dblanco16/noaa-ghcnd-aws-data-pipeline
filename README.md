# Ingeniería de Datos en AWS: Análisis y Predicción de Precipitaciones

Proyecto de Ingeniería de Datos en AWS utilizando información meteorológica histórica de **NOAA Global Historical Climatology Network Daily (GHCN-D)**.

El objetivo es implementar un pipeline de datos de extremo a extremo que permita procesar y analizar precipitaciones históricas y desarrollar un modelo predictivo de precipitación mensual acumulada.

La implementación inicial utiliza datos de un pais determinado correspondientes al período 2016–2025, con evaluación predictiva sobre datos disponibles de 2026. La arquitectura está diseñada para permitir futuras ampliaciones geográficas y temporales.

## Tecnologías

- **Python:** ingesta de datos.
- **Amazon S3:** almacenamiento.
- **AWS Glue:** procesamiento ETL y catalogación.
- **Amazon Athena:** análisis mediante SQL.
- **Amazon SageMaker Canvas:** Machine Learning.
- **AWS IAM, CloudWatch y CloudTrail:** seguridad y monitoreo.

## Documentación

- [Arquitectura y decisiones técnicas](docs/01_arquitectura.md)
- [ETL - AWS Glue](docs/02_etl.md)
- [Data Catalog - AWS Glue](docs/03_catalog.md)

## Estado del proyecto

- Arquitectura definida y configuración inicial de seguridad AWS completada. 
- Almacenamiento e ingesta de datos en S3 raw.
- ETL con Glue procesando resultados en S3 processed.
- Glue Data Catalog configurado: dos tablas externas Parquet y sincronización automática de particiones mediante Python y boto3.
- Próxima etapa: consultas SQL con Amazon Athena.

**En desarrollo.**

## Fuente de datos

[NOAA GHCN-D — AWS Open Data](https://github.com/awslabs/open-data-docs/tree/main/docs/noaa/noaa-ghcn)