#!/bin/bash
set -xe

INPUT_DIR="$HOME/bitcoin/input"
OUTPUT_DIR="$HOME/bitcoin/output"

mkdir -p $INPUT_DIR
rm -rf $INPUT_DIR/*

# following command assumes the default bitcoin client to be installed and present in the home directory
# copy the first 3 block files
cp $HOME/.bitcoin/blocks/blk0000{1..5}.dat $INPUT_DIR

#cp $HOME/.bitcoin/blocks/blk00003.dat $INPUT_DIR


hadoop fs -rm -R -f $OUTPUT_DIR

sbt clean assembly

spark-submit \
  --driver-memory 2G \
  --executor-memory 2G \
  --class io.radanalytics.bitcoin.ParquetConverter \
  --master local[8] \
  ./target/scala-2.11/bitcoin-insights.jar $INPUT_DIR $OUTPUT_DIR $@
