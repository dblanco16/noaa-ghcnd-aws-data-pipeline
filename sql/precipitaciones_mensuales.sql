-- Caso de uso: evolución mensual de precipitaciones por estación en Nueva Zelanda.
-- Fuente: noaa_ghcnd.precipitaciones_mensuales (Glue Data Catalog).
-- Período histórico: 2016-2025. El año 2026 se reserva para la etapa de ML.
-- Los acumulados suman únicamente los días observados; revisar cobertura_pct.
SELECT
    STATION AS estacion,
    anio,
    mes,
    prcp_acumulada_mm AS precipitacion_mm,
    dias_observados,
    dias_mes,
    ROUND(cobertura_pct, 2) AS cobertura_pct
FROM noaa_ghcnd.precipitaciones_mensuales
WHERE pais = 'NZ'
  AND anio BETWEEN 2016 AND 2025
ORDER BY STATION, anio, mes;
