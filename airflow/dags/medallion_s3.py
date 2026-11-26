"""Orquestra o medalhao: Airflow dispara o PySpark que le e grava no S3 emulado."""

from __future__ import annotations

import os
import urllib.error
import urllib.request
from datetime import datetime

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from airflow import DAG
from airflow.operators.python import PythonOperator

BUCKET = os.environ.get("LAKE_BUCKET", "lakehouse")
SPARK_JOB_URL = os.environ.get("SPARK_JOB_URL", "http://127.0.0.1:8091")
LAYERS = ("bronze/pedidos/", "silver/pedidos/", "gold/pedidos_por_status/")


def s3_client():
    return boto3.client(
        "s3",
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
        endpoint_url=os.environ["AWS_ENDPOINT_URL"],
        config=Config(
            s3={"addressing_style": "path"},
            inject_host_prefix=False,
            retries={"max_attempts": 3, "mode": "standard"},
        ),
    )


def criar_bucket() -> None:
    client = s3_client()
    try:
        client.create_bucket(Bucket=BUCKET)
        print(f"bucket criado: {BUCKET}")
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code not in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
            raise
        print(f"bucket ja existe: {BUCKET}")


def executar_estagio(stage: str) -> None:
    request = urllib.request.Request(
        f"{SPARK_JOB_URL}/jobs/{stage}",
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=900) as response:
            body = response.read().decode("utf-8", errors="replace")
            print(body)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        print(body)
        raise RuntimeError(f"estagio {stage} falhou com HTTP {exc.code}") from exc


def validar_s3() -> None:
    client = s3_client()
    for prefix in LAYERS:
        keys = []
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
        parquet = [key for key in keys if key.endswith(".parquet")]
        print(f"{prefix}: {len(parquet)} arquivo(s) parquet")
        for key in keys:
            print(f"  {key}")
        if not parquet:
            raise RuntimeError(f"nenhum parquet em s3://{BUCKET}/{prefix}")


with DAG(
    dag_id="medallion_s3",
    description="Bronze, silver e gold com PySpark no S3 do AWS Emulator",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["medalha", "s3", "pyspark"],
) as dag:
    bucket = PythonOperator(task_id="criar_bucket", python_callable=criar_bucket)
    bronze = PythonOperator(
        task_id="bronze",
        python_callable=executar_estagio,
        op_args=["bronze"],
    )
    silver = PythonOperator(
        task_id="silver",
        python_callable=executar_estagio,
        op_args=["silver"],
    )
    gold = PythonOperator(
        task_id="gold",
        python_callable=executar_estagio,
        op_args=["gold"],
    )
    validar = PythonOperator(task_id="validar_s3", python_callable=validar_s3)

    bucket >> bronze >> silver >> gold >> validar
