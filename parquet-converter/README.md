### Building

Make sure your sbt version is at least `0.13.5`

```bash
cd
sbt clean assembly
```
will create a jar file in `./target/scala-2.11/bitcoin-insights.jar`

### Running

Converter assumes the binary block data in the input directory and writes parquet files to the output directory. Paths to input and output directories are passed to the converter. Depending on the size of the task, or in other words, how many blocks you want to convert, tweak the memory parameters of the following example command:
```bash
ls ~/bitcoin/input/
# blk00003.dat

spark-submit \
  --driver-memory 2G \
  --executor-memory 2G \
  --class io.radanalytics.bitcoin.ParquetConverter \
  --master local[8] \
  ./target/scala-2.11/bitcoin-insights.jar ~/bitcoin/input ~/bitcoin/output

# ... <output from the conversion> ...

ls ~/bitcoin/output
# addresses  blocks  edges  transactions
```
