# Predicción de precipitaciones mensuales con SageMaker Canvas

## Objetivo y alcance

Se desarrolló un **modelo global de regresión** para estimar la precipitación acumulada del mes siguiente en estaciones meteorológicas conocidas por país (NOAA GHCN-D).

El modelo se construyó en **Amazon SageMaker Canvas (Quick build)**, región `us-east-1`, a partir del dataset mensual procesado en etapas anteriores. La preparación se realizó mediante Amazon Athena.

## Preparación y controles

- Se aceptaron únicamente meses con `dias_observados = dias_mes`. Los acumulados parciales no se trataron como totales y no se imputaron precipitaciones faltantes con cero.
- Se vinculó cada mes con el mes calendario inmediatamente anterior **de la misma estación**. La unión por `anio * 12 + mes` evita confundir la fila anterior con el mes anterior cuando existen discontinuidades.
- Predictores finales: `STATION` (categórico), `mes` y `prcp_mes_anterior`. La columna `anio` se mantuvo para la separación y trazabilidad, pero se **excluyó del entrenamiento** en Canvas.
- Objetivo: `prcp_objetivo`, equivalente a `prcp_acumulada_mm` del mes a predecir.
- Horizon: un mes; la predicción utiliza datos conocidos hasta el cierre del mes previo.
- Caso de uso: [Nueva Zelanda](../scripts/dataset_ml.sql). El resultado contiene **673 ejemplos completos**, sin valores nulos ni duplicados estación-año-mes, distribuidos temporalmente:

| Conjunto | Años | Registros |
|---|---|---:|
| Entrenamiento | 2016–2023 | 612 |
| Validación temporal | 2024–2025 | 53 |
| Evaluación final | 2026 | 8 |

En el período final, los ocho ejemplos corresponden a cuatro estaciones con observaciones en agosto y septiembre de 2026. Esto limita la representatividad de las métricas.

## Entrenamiento en Canvas

Se importó el CSV de entrenamiento (612 filas) y se creó el modelo `noaa-precipitacion-mensual`, versión 1, modalidad **Numeric prediction / Quick build**. Variable objetivo: `prcp_objetivo`. Se seleccionaron tres predictores: `STATION`, `mes` y `prcp_mes_anterior` (sin `anio`).

La evaluación **interna y automática** de Canvas arrojó **RMSE = 53,176 mm** y **MSE = 2.827,637 mm²**. El impacto relativo mostrado por Canvas fue: `STATION` 36,567 %, `prcp_mes_anterior` 35,471 % y `mes` 27,962 %. Estos porcentajes no equivalen a exactitud predictiva.

> La división interna del Quick build no fue controlada como división temporal. Por lo tanto, se informan separadamente las evaluaciones externas con años posteriores, que no se incorporaron al CSV de entrenamiento.

## Evaluación temporal externa

Se generaron predicciones por lotes en Canvas sobre los conjuntos separados de validación y evaluación final. Se compararon las predicciones con los acumulados observados por clave `STATION`, `anio`, `mes` y `prcp_mes_anterior`.

Referencia (*baseline*): estimar que el próximo mes tendrá la misma precipitación que el mes inmediatamente anterior.

| Período | N | MAE Canvas (mm) | RMSE Canvas (mm) | MAE referencia (mm) | RMSE referencia (mm) |
|---|---:|---:|---:|---:|---:|
| Validación 2024–2025 | 53 | 39,93 | 48,83 | 43,17 | 58,16 |
| Evaluación 2026 | 8 | 26,56 | 31,76 | 22,45 | 30,10 |

En validación, Canvas superó la referencia en ambas métricas. En 2026, **no la superó**. El conjunto final, con solo ocho observaciones, no permite generalizar el desempeño a todas las estaciones o estaciones del año. No se realizaron nuevos entrenamientos ni optimización de hiperparámetros.

## Integración

Se configuró un dominio de SageMaker AI con autenticación IAM, un rol de ejecución específico y acceso mediante el usuario IAM `de_noaa`.

## Limitaciones

- La consulta SQL está versionada; la separación temporal se realiza filtrando el campo `conjunto` en el CSV exportado.
- No se garantiza repetibilidad exacta del modelo Quick build por ser entrenamiento administrado con selección interna automática.
- Los faltantes redujeron el número de ejemplos. El modelo solo aplica a estaciones representadas en el entrenamiento y tiene horizonte de un mes.
- Este ejercicio demuestra integración de Ingeniería de Datos con ML, no capacidad operativa de pronóstico meteorológico.