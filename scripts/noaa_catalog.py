"""Sincroniza particiones pais/anio de NOAA PROCESSED con AWS Glue Data Catalog.

Por defecto solo muestra el plan; 
--apply crea particiones.
--prune --apply permite borrar particiones obsoletas SOLO si ambas tablas tienen Parquet.
"""
import argparse
from copy import deepcopy

import boto3

BUCKET = "de-noaa-processed-dblanco16"
DATABASE = "noaa_ghcnd"
TABLE_PREFIXES = {
    "observaciones_diarias": "ghcnd/diario/",
    "precipitaciones_mensuales": "ghcnd/mensual/",
}


def s3_partitions(s3, prefix):
    """Devuelve {(pais, anio): s3_uri} únicamente si hay archivos .parquet."""
    result = {}
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.lower().endswith(".parquet"):
                continue
            tail = key[len(prefix):].split("/")
            if len(tail) != 3 or not tail[0].startswith("pais=") or not tail[1].startswith("anio="):
                raise ValueError(f"Parquet fuera de la estructura esperada: {key}")
            pais = tail[0].split("=", 1)[1]
            anio = tail[1].split("=", 1)[1]
            if not pais or not anio.isdigit():
                raise ValueError(f"Partición inválida: {key}")
            result[(pais, anio)] = f"s3://{BUCKET}/{prefix}{tail[0]}/{tail[1]}/"
    return result


def catalog_partitions(glue, table):
    result = {}
    paginator = glue.get_paginator("get_partitions")
    for page in paginator.paginate(DatabaseName=DATABASE, TableName=table):
        for p in page.get("Partitions", []):
            result[tuple(p["Values"])] = p["StorageDescriptor"].get("Location", "")
    return result


def sync(glue, table, actual, registered, apply, prune):
    info = glue.get_table(DatabaseName=DATABASE, Name=table)["Table"]
    assert [p["Name"] for p in info["PartitionKeys"]] == ["pais", "anio"], f"Claves incorrectas: {table}"
    if "parquet" not in str(info["StorageDescriptor"]).lower():
        raise ValueError(f"La tabla no parece estar configurada como Parquet: {table}")
    missing = sorted(actual.keys() - registered.keys())
    stale = sorted(registered.keys() - actual.keys())
    changed = sorted(k for k in actual.keys() & registered.keys() if actual[k].rstrip('/') != registered[k].rstrip('/'))
    if changed:
        raise ValueError(f"Ubicaciones inconsistentes en {table}: {changed}. Revisar antes de sincronizar.")
    print(f"\n{table}: S3={len(actual)}, catálogo={len(registered)}, nuevas={len(missing)}, obsoletas={len(stale)}")
    for k in missing:
        print(f"  + {k[0]}/{k[1]}")
    if prune:
        for k in stale:
            print(f"  - {k[0]}/{k[1]}")
    if not apply:
        return
    sd = info["StorageDescriptor"]
    fields = ("Columns", "InputFormat", "OutputFormat", "SerdeInfo", "Compressed", "Parameters")
    for k in missing:
        desc = {field: deepcopy(sd[field]) for field in fields if field in sd}
        desc["Location"] = actual[k]
        response = glue.batch_create_partition(
            DatabaseName=DATABASE, TableName=table,
            PartitionInputList=[{"Values": list(k), "StorageDescriptor": desc}],
        )
        if response.get("Errors"):
            raise RuntimeError(response["Errors"])
    if prune:
        for k in stale:
            response = glue.batch_delete_partition(
                DatabaseName=DATABASE, TableName=table,
                PartitionsToDelete=[{"Values": list(k)}],
            )
            if response.get("Errors"):
                raise RuntimeError(response["Errors"])


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--profile", default="de_noaa")
    p.add_argument("--region", default="us-east-1")
    p.add_argument("--apply", action="store_true", help="Aplicar cambios; sin esto solo informa")
    p.add_argument("--prune", action="store_true", help="Eliminar particiones obsoletas (requiere --apply)")
    args = p.parse_args()
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    s3, glue = session.client("s3"), session.client("glue")
    plans = {}
    for table, prefix in TABLE_PREFIXES.items():
        actual = s3_partitions(s3, prefix)
        registered = catalog_partitions(glue, table)
        plans[table] = (actual, registered)
    if args.prune and any(not pair[0] for pair in plans.values()):
        raise SystemExit("Se bloquea --prune: alguna tabla no tiene Parquet en S3.")
    print("Modo:", "APLICACIÓN" if args.apply else "SIMULACIÓN")
    for table, (actual, registered) in plans.items():
        sync(glue, table, actual, registered, args.apply, args.prune)
    print("\nTerminado.")


if __name__ == "__main__":
    main()