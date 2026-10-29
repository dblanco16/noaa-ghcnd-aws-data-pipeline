# Ingeniería de Datos en AWS: Análisis y Predicción de Precipitaciones

Proyecto de Ingeniería de Datos en AWS utilizando información meteorológica histórica de **NOAA Global Historical Climatology Network Daily (GHCN-D)**.

El objetivo es implementar un pipeline de datos de extremo a extremo que permita procesar y analizar precipitaciones históricas y desarrollar un modelo predictivo de precipitación mensual acumulada.

La implementación inicial utiliza datos de Sudamérica correspondientes al período 2016–2025, con evaluación predictiva sobre datos disponibles de 2026. La arquitectura está diseñada para permitir futuras ampliaciones geográficas y temporales.

## Tecnologías

- **Python:** ingesta de datos.
- **Amazon S3:** almacenamiento.
- **AWS Glue:** procesamiento ETL y catalogación.
- **Amazon Athena:** análisis mediante SQL.
- **Amazon SageMaker Canvas:** Machine Learning.
- **AWS IAM, CloudWatch y CloudTrail:** seguridad y monitoreo.

## Arquitectura

[Ver documentación de arquitectura](docs/architecture.md)

## Documentación

- [Arquitectura y decisiones técnicas](docs/architecture.md)
- [Implementación y validaciones](docs/implementation-log.md)

## Estado del proyecto

- Arquitectura definida y configuración inicial de seguridad AWS completada. 
- Próxima etapa: almacenamiento e ingesta de datos.

**En desarrollo.**

## Fuente de datos

[NOAA GHCN-D — AWS Open Data](https://github.com/awslabs/open-data-docs/tree/main/docs/noaa/noaa-ghcn)