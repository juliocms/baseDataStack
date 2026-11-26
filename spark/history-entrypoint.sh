#!/bin/sh
set -eu

exec "${SPARK_HOME:-/opt/spark}/bin/spark-class" \
  org.apache.spark.deploy.history.HistoryServer
