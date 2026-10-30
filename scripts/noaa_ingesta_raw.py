"""Ingesta parametrizable NOAA GHCN-D (S3 público -> S3 RAW privado).

Usar 
--subir para efectuar la carga
--sobrescribir junto con --subir para actualizar archivos existentes.
--limite-estaciones para prueba pequeña.
"""
import argparse
import csv
import gzip
import io
import tempfile
from pathlib import Path
from datetime import date

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError

ORIGEN = "noaa-ghcn-pds"
PREFIJO = "csv.gz/by_station/"
DESTINO_PREDETERMINADO = "de-noaa-raw-dblanco16"
CAMPOS = ["ID", "DATE", "ELEMENT", "DATA_VALUE", "M_FLAG", "Q_FLAG", "S_FLAG", "OBS_TIME"]

def argumentos():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pais", default="AR", help="Código GHCN-D, por ejemplo AR")
    parser.add_argument("--desde", default="2016-01-01")
    parser.add_argument("--hasta", default="2026-12-31")
    parser.add_argument("--variables", nargs="+", default=["PRCP", "TMAX", "TMIN", "TAVG", "SNOW"])
    parser.add_argument("--bucket-destino", default=DESTINO_PREDETERMINADO)
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--limite-estaciones", type=int, default=None)
    parser.add_argument("--subir", action="store_true", help="Habilita escritura en S3 RAW")
    parser.add_argument("--sobrescribir", action="store_true", help="Reemplaza los CSV existentes (requiere --subir)")
    return parser.parse_args()

def existe(s3, bucket, clave):
    try:
        s3.head_object(Bucket=bucket, Key=clave)
        return True
    except ClientError as error:
        codigo = error.response.get("Error", {}).get("Code", "")
        if codigo in ("404", "NoSuchKey", "NotFound"):
            return False
        raise

def main():
    args = argumentos()
    if args.sobrescribir and not args.subir:
        raise ValueError("--sobrescribir requiere --subir")
        
    desde = date.fromisoformat(args.desde)
    hasta = date.fromisoformat(args.hasta)
    if desde > hasta:
        raise ValueError("La fecha inicial es posterior a la final")
    if args.limite_estaciones is not None and args.limite_estaciones < 1:
        raise ValueError("El límite debe ser positivo")

    pais = args.pais.upper()
    variables = {v.upper() for v in args.variables}
    desde_txt, hasta_txt = desde.strftime("%Y%m%d"), hasta.strftime("%Y%m%d")
    publico = boto3.client("s3", region_name=args.region, config=Config(signature_version=UNSIGNED))
    privado = boto3.client("s3", region_name=args.region) if args.subir else None

    # Inventario de estaciones: se usa para evitar descargar otras regiones.
    estaciones = set()
    respuesta = publico.get_object(Bucket=ORIGEN, Key="ghcnd-stations.txt")
    try:
        for linea in respuesta["Body"].iter_lines():
            identificador = linea[:11].decode("utf-8").strip()
            if identificador.startswith(pais):
                estaciones.add(identificador)
    finally:
        respuesta["Body"].close()

    # Listar únicamente los objetos comprimidos del país.
    objetos = []
    for pagina in publico.get_paginator("list_objects_v2").paginate(
        Bucket=ORIGEN, Prefix=PREFIJO + pais
    ):
        for objeto in pagina.get("Contents", []):
            clave = objeto["Key"]
            identificador = Path(clave).name.removesuffix(".csv.gz")
            if clave.endswith(".csv.gz") and identificador in estaciones:
                objetos.append((identificador, clave, objeto["Size"]))
    objetos.sort()
    if args.limite_estaciones:
        objetos = objetos[:args.limite_estaciones]
    print(f"País: {pais} | Estaciones con archivo: {len(objetos)} | Modo: {'SUBIR' if args.subir else 'SIMULACIÓN'}")
    print(f"Volumen origen comprimido: {sum(o[2] for o in objetos):,} bytes")
    print(f"Período: {args.desde} a {args.hasta} | Variables: {', '.join(sorted(variables))}")

    # En simulación no se lee ni descarga el contenido de los CSV.
    if not args.subir:
        print("Sin escrituras. Para probar, usar --limite-estaciones 2 --subir.")
        return

    filas_total = 0
    cargadas = 0
    omitidas = 0
    reemplazadas = 0
    for identificador, clave_origen, _ in objetos:
        # Prefijo estable por país y estación. Los filtros se aplican al contenido.
        clave_destino = f"ghcnd/pais={pais}/{identificador}.csv"
        ya_existe = existe(privado, args.bucket_destino, clave_destino)
        if ya_existe and not args.sobrescribir:
            omitidas += 1
            print(f"OMITIDO (ya existe): {identificador}")
            continue

       # Archivo temporal por estación: no se conserva copia al terminar.
        with tempfile.TemporaryFile(mode="w+b") as temporal:
            respuesta = publico.get_object(Bucket=ORIGEN, Key=clave_origen)
            n = 0
            try:
                with gzip.GzipFile(fileobj=respuesta["Body"]) as descomprimido:
                    with io.TextIOWrapper(descomprimido, encoding="utf-8", newline="") as entrada:
                        salida = io.TextIOWrapper(temporal, encoding="utf-8", newline="", write_through=True)
                        try:
                            escritor = csv.writer(salida, lineterminator="\n")
                            escritor.writerow(CAMPOS)
                            lector = csv.reader(entrada)
                            for fila in lector:
                                if fila and fila[0].strip().upper() == "ID":
                                    continue
                                if len(fila) < 4:
                                    raise ValueError(f"Fila mal formada en {clave_origen}")
                                fecha = fila[1].strip().replace("-", "")
                                if desde_txt <= fecha <= hasta_txt and fila[2].strip() in variables:
                                    escritor.writerow((fila + [""] * (len(CAMPOS) - len(fila)))[:len(CAMPOS)])
                                    n += 1
                        finally:
                            salida.flush()
                            salida.detach()
            finally:
                respuesta["Body"].close()
            if n:
                temporal.seek(0)
                privado.upload_fileobj(temporal, args.bucket_destino, clave_destino,
                                       ExtraArgs={"ContentType": "text/csv"})
                if ya_existe:
                    reemplazadas += 1
                    print(f"REEMPLAZADO: {identificador}: {n:,} filas")
                else:
                    cargadas += 1
                    print(f"CARGADO: {identificador}: {n:,} filas")
                filas_total += n
            else:
                print(f"SIN FILAS (sin cambios en S3): {identificador}")
    print(f"Total: {cargadas} nuevos, {reemplazadas} reemplazados, {omitidas} omitidos, {filas_total:,} filas subidas")

if __name__ == "__main__":
    main()