"""ETL NOAA GHCN-D: CSV de S3 RAW a Parquet diario y mensual en S3 PROCESSED.

Compatible con Python 3.9 (AWS Glue Python Shell). Requiere boto3 y pyarrow.
Procesa una estación por vez y consolida un archivo Parquet Snappy por país/año/dataset.
No utiliza Data Catalog ni Crawler.
"""

import argparse
import csv
import io
import logging
import tempfile
from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

RAW_PREDETERMINADO = "de-noaa-raw-dblanco16"
PROCESSED_PREDETERMINADO = "de-noaa-processed-dblanco16"
FACTORES = {"PRCP": 0.1, "TMAX": 0.1, "TMIN": 0.1, "TAVG": 0.1, "SNOW": 1.0}
COLUMNAS_RAW = ["STATION", "DATE", "ELEMENT", "DATA_VALUE", "M_FLAG", "Q_FLAG", "S_FLAG", "OBS_TIME"]
COLUMNAS_DIARIO = ["STATION", "DATE", "ELEMENT", "valor_original", "valor", "M_FLAG", "S_FLAG", "OBS_TIME"]
COLUMNAS_MENSUAL = ["STATION", "mes", "dias_mes", "dias_observados", "cobertura_pct", "prcp_acumulada_mm"]
SCHEMA_DIARIO = pa.schema([
    ("STATION", pa.string()), ("DATE", pa.date32()), ("ELEMENT", pa.string()),
    ("valor_original", pa.int32()), ("valor", pa.float64()),
    ("M_FLAG", pa.string()), ("S_FLAG", pa.string()), ("OBS_TIME", pa.string()),
])
SCHEMA_MENSUAL = pa.schema([
    ("STATION", pa.string()), ("mes", pa.int32()), ("dias_mes", pa.int32()),
    ("dias_observados", pa.int32()), ("cobertura_pct", pa.float64()),
    ("prcp_acumulada_mm", pa.float64()),
])
LOG = logging.getLogger("noaa_etl")


def argumentos():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pais", default="NZ")
    parser.add_argument("--desde", default="2016-01-01")
    parser.add_argument("--hasta", default="2026-12-31")
    parser.add_argument("--variables", nargs="+", default=list(FACTORES))
    parser.add_argument("--bucket-raw", default=RAW_PREDETERMINADO)
    parser.add_argument("--bucket-processed", default=PROCESSED_PREDETERMINADO)
    args, extras = parser.parse_known_args()
    # Glue puede añadir argumentos del propio servicio (JOB_NAME, etc.).
    if extras:
        LOG.info("Argumentos adicionales ignorados: %s", extras)
    args.pais = args.pais.strip().upper()
    args.variables = list(dict.fromkeys(v.upper() for v in args.variables))
    try:
        args.desde = date.fromisoformat(args.desde)
        args.hasta = date.fromisoformat(args.hasta)
    except ValueError as exc:
        parser.error(str(exc))
    if args.desde > args.hasta:
        parser.error("--desde debe ser anterior o igual a --hasta")
    if not args.pais or not args.variables or not set(args.variables) <= set(FACTORES):
        parser.error("País o variables inválidas")
    if not args.bucket_raw or not args.bucket_processed or args.bucket_raw == args.bucket_processed:
        parser.error("Los buckets RAW y PROCESSED deben ser distintos y no vacíos")
    return args


def listar_estaciones(s3, bucket, pais):
    prefijo = "ghcnd/pais={}/".format(pais)
    for pagina in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefijo):
        for obj in pagina.get("Contents", []):
            clave = obj["Key"]
            if clave.endswith(".csv"):
                yield clave


def dias_del_mes(anio, mes):
    if mes == 12:
        siguiente = date(anio + 1, 1, 1)
    else:
        siguiente = date(anio, mes + 1, 1)
    return (siguiente - date(anio, mes, 1)).days


def volcar_estacion(s3, bucket, clave, args, temporal, estadisticas, particiones):
    """Lee una estación y agrega filas a spools CSV locales por año y dataset."""
    id_archivo = Path(clave).stem
    respuesta = s3.get_object(Bucket=bucket, Key=clave)
    # Memoria acotada: solo mantiene un resumen de PRCP mensual de esta estación.
    precipitacion = defaultdict(lambda: [0, 0.0])
    vistas = set()  # (fecha, variable) de la estación, para impedir duplicación
    try:
        with io.TextIOWrapper(respuesta["Body"], encoding="utf-8-sig", newline="") as entrada:
            lector = csv.DictReader(entrada)
            if lector.fieldnames != COLUMNAS_RAW:
                raise ValueError("Encabezado inesperado en {}: {}".format(clave, lector.fieldnames))
            for numero, fila in enumerate(lector, start=2):
                estadisticas["leidos"] += 1
                try:
                    if None in fila or any(x is None for x in fila.values()):
                        raise ValueError("Cantidad de columnas incorrecta")
                    estacion = fila["STATION"].strip()
                    if estacion != id_archivo:
                        raise ValueError("STATION no coincide con el nombre del archivo")
                    fecha = datetime.strptime(fila["DATE"].strip(), "%Y%m%d").date()
                    variable = fila["ELEMENT"].strip()
                except (ValueError, TypeError, AttributeError) as exc:
                    raise ValueError("{} fila {}: {}".format(clave, numero, exc)) from exc
                if not args.desde <= fecha <= args.hasta or variable not in args.variables:
                    estadisticas["fuera_alcance"] += 1
                    continue
                identificador = (fecha, variable)
                if identificador in vistas:
                    raise ValueError("{} fila {}: estación/fecha/variable duplicadas".format(clave, numero))
                vistas.add(identificador)
                valor_txt = fila["DATA_VALUE"].strip()
                if valor_txt in ("", "-9999"):
                    estadisticas["descartados_sin_valor"] += 1
                    continue
                try:
                    valor_original = int(valor_txt)
                    if not -(2**31) <= valor_original < 2**31:
                        raise ValueError("fuera de rango int32")
                except ValueError:
                    estadisticas["descartados_no_numericos"] += 1
                    continue
                if fila["Q_FLAG"].strip():
                    estadisticas["descartados_calidad"] += 1
                    continue
                valor = valor_original * FACTORES[variable]
                fila_diaria = [estacion, fecha.isoformat(), variable, valor_original, valor,
                               fila["M_FLAG"].strip(), fila["S_FLAG"].strip(), fila["OBS_TIME"].strip()]
                temporal.agregar("diario", fecha.year, fila_diaria)
                particiones["diario"].add(fecha.year)
                estadisticas["validos"] += 1
                if variable == "PRCP":
                    mes = (fecha.year, fecha.month)
                    precipitacion[mes][0] += 1
                    precipitacion[mes][1] += valor
    finally:
        respuesta["Body"].close()

    for (anio, mes), (observados, acumulada) in sorted(precipitacion.items()):
        dias = dias_del_mes(anio, mes)
        if observados > dias:
            raise ValueError("Más observaciones que días: {} {}-{}".format(id_archivo, anio, mes))
        temporal.agregar("mensual", anio, [id_archivo, mes, dias, observados,
                                         round(observados / dias * 100, 6), round(acumulada, 6)])
        particiones["mensual"].add(anio)
        estadisticas["meses"] += 1


class Spools:
    """Archivos intermedios por dataset/año. Nunca acumula todas las filas en RAM."""

    def __init__(self, carpeta):
        self.carpeta = Path(carpeta)
        self.archivos = {}
        self.escritores = {}

    def agregar(self, dataset, anio, fila):
        clave = (dataset, anio)
        if clave not in self.archivos:
            ruta = self.carpeta / "{}-{}.csv".format(dataset, anio)
            fh = ruta.open("w", newline="", encoding="utf-8")
            self.archivos[clave] = fh
            self.escritores[clave] = csv.writer(fh)
        self.escritores[clave].writerow(fila)

    def cerrar(self):
        for fh in self.archivos.values():
            fh.close()
        self.archivos.clear()
        self.escritores.clear()


def convertir_celda(dataset, fila):
    if dataset == "diario":
        return [fila[0], date.fromisoformat(fila[1]), fila[2], int(fila[3]), float(fila[4]),
                fila[5], fila[6], fila[7]]
    return [fila[0], int(fila[1]), int(fila[2]), int(fila[3]), float(fila[4]), float(fila[5])]


def csv_a_parquet(entrada, salida, dataset, lote=10000):
    """Escribe un solo Parquet mediante lotes, sin cargar el CSV entero."""
    esquema = SCHEMA_DIARIO if dataset == "diario" else SCHEMA_MENSUAL
    nombres = COLUMNAS_DIARIO if dataset == "diario" else COLUMNAS_MENSUAL
    total = 0
    with pq.ParquetWriter(str(salida), esquema, compression="snappy") as escritor:
        with Path(entrada).open("r", newline="", encoding="utf-8") as f:
            lector = csv.reader(f)
            while True:
                filas = []
                for _ in range(lote):
                    try:
                        filas.append(convertir_celda(dataset, next(lector)))
                    except StopIteration:
                        break
                if not filas:
                    break
                columnas = list(zip(*filas))
                tabla = pa.Table.from_arrays(
                    [pa.array(col, type=esquema.field(i).type) for i, col in enumerate(columnas)],
                    names=nombres,
                )
                escritor.write_table(tabla)
                total += len(filas)
    if total == 0:
        raise ValueError("Partición sin registros: {}".format(entrada))
    return total


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = argumentos()
    s3 = boto3.client("s3", region_name="us-east-1")
    claves = list(listar_estaciones(s3, args.bucket_raw, args.pais))
    if not claves:
        raise ValueError("No se encontraron CSV RAW para país {}".format(args.pais))
    LOG.info("País %s; estaciones RAW %s; período %s a %s; variables %s",
             args.pais, len(claves), args.desde, args.hasta, ",".join(args.variables))
    estadisticas = Counter()
    particiones = {"diario": set(), "mensual": set()}
    with tempfile.TemporaryDirectory(prefix="noaa-etl-") as carpeta:
        carpeta = Path(carpeta)
        spools = Spools(carpeta)
        try:
            for numero, clave in enumerate(claves, 1):
                volcar_estacion(s3, args.bucket_raw, clave, args, spools, estadisticas, particiones)
                LOG.info("Procesada estación %s/%s: %s", numero, len(claves), clave)
        finally:
            spools.cerrar()
        # Generar y validar todos los Parquet antes de publicar uno solo.
        archivos = []
        for dataset in ("diario", "mensual"):
            for anio in sorted(particiones[dataset]):
                origen = carpeta / "{}-{}.csv".format(dataset, anio)
                destino = carpeta / "{}-{}.parquet".format(dataset, anio)
                cantidad = csv_a_parquet(origen, destino, dataset)
                if pq.ParquetFile(str(destino)).metadata.num_rows != cantidad:
                    raise ValueError("Cantidad de filas incorrecta en {}".format(destino))
                clave = "ghcnd/{}/pais={}/anio={}/datos.parquet".format(dataset, args.pais, anio)
                archivos.append((destino, clave, cantidad))
        if not archivos or not particiones["diario"]:
            raise ValueError("No se generaron resultados: revisar parámetros y cobertura RAW")
        # Identificar particiones anteriores del mismo país para reemplazo completo.
        anteriores = set()
        for dataset in ("diario", "mensual"):
            prefijo = "ghcnd/{}/pais={}/".format(dataset, args.pais)
            for pagina in s3.get_paginator("list_objects_v2").paginate(
                Bucket=args.bucket_processed, Prefix=prefijo
            ):
                for objeto in pagina.get("Contents", []):
                    anteriores.add(objeto["Key"])
        nuevas = {clave for _, clave, _ in archivos}
        # El reemplazo de múltiples objetos S3 NO es atómico.
        LOG.info("Publicación de %s archivos Parquet (S3 no ofrece transacción multiobjeto)", len(archivos))
        for ruta, clave, cantidad in archivos:
            s3.upload_file(str(ruta), args.bucket_processed, clave)
            verificacion = s3.head_object(Bucket=args.bucket_processed, Key=clave)
            if verificacion["ContentLength"] != ruta.stat().st_size:
                raise RuntimeError("Tamaño distinto tras subir {}".format(clave))
            LOG.info("PUBLICADO s3://%s/%s | filas=%s", args.bucket_processed, clave, cantidad)
        # Retirar objetos obsoletos solo después de publicar y comprobar todos los nuevos.
        for clave in sorted(anteriores - nuevas):
            s3.delete_object(Bucket=args.bucket_processed, Key=clave)
            LOG.info("ELIMINADO objeto anterior: %s", clave)
    LOG.info("RESUMEN: %s", dict(estadisticas))


if __name__ == "__main__":
    main()
