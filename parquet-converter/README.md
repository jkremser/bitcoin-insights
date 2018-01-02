### Building

Make sure your sbt version is at least `0.13.5`

```bash
cd 
sbt clean assembly
```
will create a jar file in `./target/scala-2.11/bitcoin-insights.jar`

### Running

Converter assumes the data in the HDFS and writes two parquet files (one with nodes and one with edges) back to the HDFS. Depending on the size of the task, or in other words, how many blocks you want to convert, tweak the memory parameters of the following command:
```bash
spark-submit \
  --driver-memory 2G \
  --executor-memory 2G \
  --class io.radanalytics.bitcoin.ParquetConverter \
  --master local[8] \
  ./target/scala-2.11/bitcoin-insights.jar /user/cloudera/bitcoin/input /user/cloudera/bitcoin/output
```

Also make sure the `/user/cloudera/bitcoin/output` is empty before running the previous command:

```bash
hadoop fs -rm -R -f /user/cloudera/bitcoin/output/
```
