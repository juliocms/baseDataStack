#!/usr/bin/env python3
"""Pipeline medalhao: bronze, silver e gold no S3 do AWS Emulator."""

from __future__ import annotations

import argparse
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

BUCKET = os.environ.get("LAKE_BUCKET", "lakehouse")
CREDENTIALS = os.environ.get(
    "AWS_SHARED_CREDENTIALS_FILE",
    "/aws/emulator/9297/emulator_credentials",
)
ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "https://127.0.0.1:9297")
REGION = os.environ.get("AWS_REGION", "us-east-1")


def load_keys(path: str) -> tuple[str, str]:
    access = secret = None
    with open(path, encoding="utf-8") as handle:
        for raw in handle:
            line = raw.strip()
            if line.startswith("aws_access_key_id"):
                access = line.split("=", 1)[1].strip()
            elif line.startswith("aws_secret_access_key"):
                secret = line.split("=", 1)[1].strip()
    if not access or not secret:
        raise SystemExit(f"credenciais ausentes em {path}")
    return access, secret


def build_spark(access: str, secret: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName("medallion-s3")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT)
        .config("spark.hadoop.fs.s3a.endpoint.region", REGION)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        .config("spark.hadoop.fs.s3a.access.key", access)
        .config("spark.hadoop.fs.s3a.secret.key", secret)
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.bucket.probe", "0")
        .config("spark.hadoop.fs.s3a.change.detection.mode", "none")
        .config("spark.hadoop.fs.s3a.change.detection.version.required", "false")
        .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "false")
        .config("spark.hadoop.fs.s3a.fast.upload", "false")
        .config("spark.hadoop.fs.s3a.multipart.threshold", "536870912")
        .config("spark.hadoop.fs.s3a.multipart.size", "536870912")
        .config("spark.hadoop.fs.s3a.committer.name", "directory")
        .config("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "replace")
        .config("spark.hadoop.fs.s3a.committer.staging.tmp.path", "/tmp/spark-staging")
        .config(
            "spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a",
            "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory",
        )
        .config(
            "spark.sql.sources.commitProtocolClass",
            "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol",
        )
        .config(
            "spark.sql.parquet.output.committer.class",
            "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter",
        )
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def path(layer: str, dataset: str) -> str:
    return f"s3a://{BUCKET}/{layer}/{dataset}"


def write(frame: DataFrame, destination: str) -> None:
    (
        frame.coalesce(1)
        .write.mode("overwrite")
        .format("parquet")
        .save(destination)
    )


def bronze(spark: SparkSession) -> None:
    schema = StructType(
        [
            StructField("pedido_id", IntegerType(), True),
            StructField("cliente", StringType(), True),
            StructField("data_pedido", StringType(), True),
            StructField("status", StringType(), True),
            StructField("valor", DoubleType(), True),
        ]
    )
    rows = [
        (1, "Ana", "2026-01-15", "pago", 120.5),
        (2, "Bruno", "2026-01-15", "PAGO", 80.0),
        (3, "Carla", "2026-01-15", "cancelado", 40.0),
        (4, None, "2026-01-16", "pago", 15.0),
        (5, "Diego", "2026-01-16", "pendente", 200.0),
        (6, "Eva", "2026-01-16", "pago", None),
        (7, "Fabio", "2026-01-17", "pago", -10.0),
        (8, "Gina", "2026-01-17", "desconhecido", 30.0),
        (9, "Hugo", "2026-01-17", "pendente", 55.25),
        (10, "Iara", None, "pago", 12.0),
    ]
    frame = spark.createDataFrame(rows, schema)
    destination = path("bronze", "pedidos")
    write(frame, destination)
    print(f"bronze gravado em {destination}: {frame.count()} linhas")
    frame.show(truncate=False)


def silver(spark: SparkSession) -> None:
    source = path("bronze", "pedidos")
    frame = spark.read.parquet(source)
    cleaned = (
        frame.withColumn("status", F.lower(F.trim(F.col("status"))))
        .withColumn("cliente", F.trim(F.col("cliente")))
        .withColumn("data_pedido", F.to_date(F.col("data_pedido")))
        .filter(F.col("pedido_id").isNotNull())
        .filter(F.col("cliente").isNotNull() & (F.length(F.col("cliente")) > 0))
        .filter(F.col("data_pedido").isNotNull())
        .filter(F.col("valor").isNotNull() & (F.col("valor") > 0))
        .filter(F.col("status").isin("pago", "pendente", "cancelado"))
        .select("pedido_id", "cliente", "data_pedido", "status", "valor")
    )
    destination = path("silver", "pedidos")
    write(cleaned, destination)
    print(f"silver gravado em {destination}: {cleaned.count()} linhas")
    cleaned.orderBy("pedido_id").show(truncate=False)


def gold(spark: SparkSession) -> None:
    source = path("silver", "pedidos")
    frame = spark.read.parquet(source)
    summary = (
        frame.groupBy("status", "data_pedido")
        .agg(
            F.count("*").alias("quantidade_pedidos"),
            F.sum("valor").alias("valor_total"),
            F.countDistinct("cliente").alias("clientes_distintos"),
        )
        .orderBy("data_pedido", "status")
    )
    destination = path("gold", "pedidos_por_status")
    write(summary, destination)
    print(f"gold gravado em {destination}: {summary.count()} linhas")
    summary.show(truncate=False)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", required=True, choices=["bronze", "silver", "gold"])
    args = parser.parse_args()

    access, secret = load_keys(CREDENTIALS)
    spark = build_spark(access, secret)
    try:
        {"bronze": bronze, "silver": silver, "gold": gold}[args.stage](spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
