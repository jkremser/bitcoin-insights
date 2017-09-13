#!/bin/bash
set -xe

hadoop fs -rm -R -f /user/cloudera/bitcoin/output/

sbt clean assembly

spark-submit \
  --driver-memory 2G \
  --executor-memory 2G \
  --class io.radanalytics.bitcoin.ParquetConverter \
  --master local[8] \
  ./target/scala-2.11/bitcoin-insights.jar /user/cloudera/bitcoin/input /user/cloudera/bitcoin/output
