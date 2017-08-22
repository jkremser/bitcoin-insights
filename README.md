# Bitcoin Insights
[![Build status](https://travis-ci.org/Jiri-Kremser/bitcoin-insights.svg?branch=master)](https://travis-ci.org/Jiri-Kremser/bitcoin-insights)

## Converter to Parguet

Bitcoin stores all the transactions in the binary format described [here](https://webbtc.com/api/schema), however Spark is much better with columnar data formats such as Parquet, so we provide a simple converter from the blockchain binary format into parquet files. The converter was inspired by the examples in the [hadoopcryptoledger](https://github.com/ZuInnoTe/hadoopcryptoledger/wiki/Using-Hive-to-analyze-Bitcoin-Blockchain-data) project.

### Building

```bash
sbt clean assembly
```
will create a jar file in `./target/scala-2.11/bitcoin-insights.jar`

### Running

Converter assumes the data in the HDFS and writes two parquet files (one with nodes and one with edges) back to the HDFS. Depending on the size of the task, or in other words, how many blocks you want to convert, tweak the memory parameters of the following command:
```bash
spark-submit --driver-memory 2G --num-executors 4 --executor-memory 2G --class io.radanalytics.bitcoin.ParquetConverter --master local[8] ./target/scala-2.11/bitcoin-insights.jar /user/cloudera/bitcoin/input /user/cloudera/bitcoin/output
```

Also make sure the `/user/cloudera/bitcoin/output` is empty before running the previous command:

```bash
hadoop fs -rm -R -f /user/cloudera/bitcoin/output/
```
