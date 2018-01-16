#!/bin/bash
set -xe

INPUT_DIR="$HOME/bitcoin/input"
OUTPUT_DIR="$HOME/bitcoin/output"

mkdir -p $INPUT_DIR
mkdir -p $HOME/tmp
rm -rf $INPUT_DIR/*

# following command assumes the default bitcoin client to be installed and present in the home directory
# copy a file that contains serialized blocks
cp $HOME/.bitcoin/blocks/blk00003.dat $INPUT_DIR

#cp $HOME/.bitcoin/blocks/blk000{0..5}{0..9}.dat $INPUT_DIR

# first 200
#cp $HOME/.bitcoin/blocks/blk00{0,1}{0..9}{0..9}.dat $INPUT_DIR
#cp $HOME/.bitcoin/blocks/blk00200.dat $INPUT_DIR

rm -Rf $OUTPUT_DIR

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
pushd $DIR
sbt clean assembly

spark-submit \
  --driver-memory 10G \
  --executor-memory 1.25G \
  --class io.radanalytics.bitcoin.ParquetConverterDS \
  --master local[4] \
  --conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
  --conf "spark.local.dir=$HOME/tmp" \
  ./target/scala-2.11/bitcoin-insights.jar $INPUT_DIR $OUTPUT_DIR $@

  popd
