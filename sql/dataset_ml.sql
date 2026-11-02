-- Etapa 6: dataset de regresión mensual a un mes, multiestación.
-- Ejecutar en Athena, workgroup noaa-ghcnd-wg.
-- No modifica las tablas originales ni crea recursos.
-- Para exportar: descargar el CSV del resultado desde Athena.
WITH base AS (
    SELECT
        STATION,
        anio,
        mes,
        prcp_acumulada_mm,
        anio * 12 + mes AS periodo
    FROM noaa_ghcnd.precipitaciones_mensuales
    WHERE pais = 'NZ'
      AND dias_observados = dias_mes
),
ejemplos AS (
    SELECT
        actual.STATION,
        actual.anio,
        actual.mes,
        anterior.prcp_acumulada_mm AS prcp_mes_anterior,
        actual.prcp_acumulada_mm AS prcp_objetivo,
        CASE
            WHEN actual.anio BETWEEN 2016 AND 2023 THEN 'entrenamiento'
            WHEN actual.anio BETWEEN 2024 AND 2025 THEN 'validacion'
            WHEN actual.anio = 2026 THEN 'evaluacion'
        END AS conjunto
    FROM base actual
    INNER JOIN base anterior
        ON actual.STATION = anterior.STATION
       AND anterior.periodo = actual.periodo - 1
    WHERE actual.anio BETWEEN 2016 AND 2026
)
SELECT
    STATION, anio, mes, prcp_mes_anterior, prcp_objetivo, conjunto
FROM ejemplos
ORDER BY anio, mes, STATION;
