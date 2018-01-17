/*
 * Copyright 2017 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
  * This simple parquet converter was inspired by example projects in https://github.com/ZuInnoTe/hadoopcryptoledger
  *
  * For the structure of the blocks consult https://webbtc.com/api/schema
  */
package io.radanalytics.bitcoin

import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.zuinnote.hadoop.bitcoin.format.common._
import org.zuinnote.hadoop.bitcoin.format.mapreduce.BitcoinBlockFileInputFormat

import scala.collection.JavaConverters
import scala.collection.mutable.MutableList

object ParquetConverterDS {
  var debug = 0

  import CustomTypes._

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Bitcoin insights - ParquetConverter)")
    val sc = new SparkContext(conf)
    val hadoopConf = new Configuration()
    if (args.size == 3) debug = args(2).toInt
    convert(sc, hadoopConf, args(0), args(1))
    sc.stop
  }

  def convert(sc: SparkContext, hadoopConf: Configuration, inputFile: String, outputDir: String) = {
    // TODO: !!! currently all the entities have ids from the same range, so there are collisions

    val spark = SparkSession
      .builder()
      .getOrCreate()
    import spark.implicits._


    if (debug >= 1) println("\n\n\n\n\n                          ********************invoking convert()****************              \n\n\n\n\n\n\n\n")
    val bitcoinBlocksRDD = sc.newAPIHadoopFile(inputFile, classOf[BitcoinBlockFileInputFormat], classOf[BytesWritable], classOf[BitcoinBlock], hadoopConf)

    val blockchainDataRDD: RDD[(Block, Array[Transaction2], Array[Input2], Array[Output2])] = bitcoinBlocksRDD.map(hadoopKeyValueTuple => extractData(hadoopKeyValueTuple._2))
    val allInputs: RDD[Input2] = blockchainDataRDD.flatMap(t => t._3)
    val allInputsDS: Dataset[Input2] = spark.createDataset(allInputs)
    val allOutputs: RDD[Output2] = blockchainDataRDD.flatMap(t => t._4)
    val allOutputsDS: Dataset[Output2] = spark.createDataset(allOutputs)

    if (debug >= 2) {
      println("\n\n\n\n\n allOutputs: RDD[Output] = \n\n")
      allOutputs.sample(true, 0.001).take(5).foreach(println)
    }

    // note: distinct causes shuffle
    val addressesCached: RDD[String] = allOutputs.map(_.address).distinct.cache
    val addresses: RDD[(String, VertexOrderId)] = addressesCached.zipWithIndex()
    val addressesDS: Dataset[(String, VertexOrderId)] = spark.createDataset(addresses).cache
    if (debug >= 2) {
      val strange = addresses.filter(a => a._1.contains("_"))
      println("\n\n\n\n\n\nNON-STANDARD #: " + strange.count())
      println("\n\n\n\n\n\nNON-STANDARD (bitcoinpubkey_)#: " + strange.filter(s => s._1.contains("bitcoinpubkey")).count())
      println("\n\n\n\n\n\nNON-STANDARD  :" + strange.sample(true, 0.1).take(5).foreach(println))
      //      other possible prefixes: "puzzle_" "anyone" "unspendable"
    }

    if (debug >= 1) {
      println("\n\n\n\n\n addresses: RDD[String] = \n\n")
      addresses.sample(true, 0.001).take(5).foreach(println)
    }

    val allBlocksCached: RDD[Block] = blockchainDataRDD.map(t => t._1).cache
    val allBlocks: RDD[(Block, BlockOrderId)] = allBlocksCached.zipWithIndex
    val allBlocksDS: Dataset[(Block, BlockOrderId)] = spark.createDataset(allBlocks)

    if (debug >= 2) {
      println("\n\n\n\n\n allBlocksDS = \n\n")
      allBlocksDS.sample(true, 0.001).take(5).foreach(println)
      allBlocksDS.show
    }

    val allTransactionsCached: RDD[Transaction2] = blockchainDataRDD.flatMap(t => t._2).cache
    val allTransactions: RDD[(Transaction2, TransactionOrderId)] = allTransactionsCached.zipWithIndex()
    val allTransactionsDS: Dataset[(Transaction2, TransactionOrderId)] = spark.createDataset(allTransactions).cache


    val addressesDF: DataFrame = addressesDS.map(a => (a._2, a._1)).toDF("id", "address")
    addressesDF.write.mode("overwrite").format("parquet").option("compression", "gzip").mode("overwrite").save(s"$outputDir/addresses")

    val allBlocksDF = allBlocksDS.map(b => (b._2, b._1.hash, b._1.time)).toDF("id", "hash", "time")
    allBlocksDF.write.mode("overwrite").format("parquet").option("compression", "gzip").mode("overwrite").save(s"$outputDir/blocks")

    val allTransactionsDF = allTransactionsDS.map(t => (t._2, t._1.hash)).toDF("id", "hash")
    allTransactionsDF.write.mode("overwrite").format("parquet").option("compression", "gzip").mode("overwrite").save(s"$outputDir/transactions")

    // |address|txRef|ord|value| : long, long, long, long
    val allOutputsAux: DataFrame = allOutputsDS.join(addressesDS, allOutputsDS("address") === addressesDS("_1"))
      .withColumnRenamed("_2", "aux").drop("_1")
      .join(allTransactionsDS, $"txRef.hash" === $"_1.hash")
      .select($"aux".as("address"), $"_2".as("txRef"), $"txRef.ord", $"value")
      .cache

    // |address|txOutputRef|ord| tx| : long, long, long, long
    val allInputsAux: DataFrame = allInputsDS.join(allTransactionsDS, $"txOutputRef.hash" === $"_1.hash")
      .select($"_2".as("txOutputRef"), $"txOutputRef.ord", $"tx")
      .join(allTransactionsDS, $"tx" === $"_1.hash")
      .drop("_1", "tx")
      .withColumnRenamed("_2", "tx")

    if (debug >= 2) {
      println("\n\n\n\n\n allInputsDS = \n\n")
      allInputsDS.sample(true, 0.001).take(5).foreach(println)
      allInputsDS.show
      println("step0:\n\n")
      println("allInputsDS:\n\n")
      allInputsDS.show
      println("step1:\n\n")
      allInputsDS.join(allTransactionsDS, $"txOutputRef.hash" === $"_1.hash")
        .show
      println("step1.5:\n\n")
      allInputsDS.join(allTransactionsDS, $"txOutputRef.hash" === $"_1.hash")
        .select($"_2".as("txOutputRef"), $"txOutputRef.ord", $"tx")
        .show
      println("step2:\n\n")
      allInputsDS.join(allTransactionsDS, $"txOutputRef.hash" === $"_1.hash")
        .select($"_2".as("txOutputRef"), $"txOutputRef.ord", $"tx")
        .join(allTransactionsDS, $"tx" === $"_1.hash")
        .show
    }

    if (debug >= 2) {
      println("\n\n\n\n\n allInputsAux = \n\n")
      allInputsAux.sample(true, 0.001).take(5).foreach(println)
      allInputsAux.show
    }


    import org.apache.spark.sql.functions._
    // block -> TX
    val blockTxEdges = allTransactionsDS.join(allBlocksDS, allTransactionsDS("_1.block") === allBlocksDS("_1.hash"))
      .drop("_1")
      .withColumn("foo" , lit(0L))
    printCount("blockTxEdges", blockTxEdges)

    // block -> previous block
    val blockBlockEdges = allBlocksDS.alias("ds1").join(allBlocksDS.alias("ds2"), $"ds1._1.hash" === $"ds2._1.prevHash")
      .drop("_1").withColumn("foo" , lit(0L))
    printCount("blockBlockEdges", blockBlockEdges)

    // TX -> address (outputs)
    val txAddressEdges = allOutputsAux.select($"txRef", $"address", $"value")
    printCount("txAddressEdges", txAddressEdges)

    // clear caches that are not needed anymore
//    allBlocksCached.unpersist(true)
//    addressesCached.unpersist(true)
//    addressesDS.unpersist(true)

    // address => TX (inputs)
    val addressTxEdges = allOutputsAux.alias("ds1")
      .join(allInputsAux.alias("ds2"), $"txOutputRef" === $"txRef" && $"ds1.ord" === $"ds2.ord")
      .select($"address", $"tx", $"value")
    printCount("addressTxEdges", addressTxEdges)

    val allEdgesDF = blockTxEdges
        .union(addressTxEdges)
        .union(txAddressEdges)
        .union(blockBlockEdges)
        .toDF("src", "dst", "value")

    allEdgesDF.repartition(2).write.mode("overwrite").format("parquet").option("compression", "gzip").mode("overwrite").save(s"$outputDir/edges")
  }

  def extractData(bitcoinBlock: BitcoinBlock): (Block, Array[Transaction2], Array[Input2], Array[Output2]) = {
    val blockTime: Int = bitcoinBlock.getTime
    val blockHashHex: String = BitcoinUtil.convertByteArrayToHexString(BitcoinUtil.getBlockHash(bitcoinBlock))
    val prevHash: String = BitcoinUtil.convertByteArrayToHexString(BitcoinUtil.reverseByteArray(bitcoinBlock.getHashPrevBlock))
    val rawTransactions: Array[TxData] = bitcoinBlock.getTransactions().toArray(Array[BitcoinTransaction]())
    val inputs: MutableList[Input2] = MutableList[Input2]()
    val outputs: MutableList[Output2] = MutableList[Output2]()

    val transactions: Array[Transaction2] = rawTransactions.map((tx: BitcoinTransaction) => {
      val txHash: Array[Byte] = BitcoinUtil.reverseByteArray(BitcoinUtil.getTransactionHash(tx))
      val txHashHex: String =  BitcoinUtil.convertByteArrayToHexString(txHash)

      inputs ++= JavaConverters.asScalaBufferConverter(tx.getListOfInputs).asScala.map(input => {
        convertBitcoinTransactionInput2Input(input, txHashHex)
      })

      outputs ++= JavaConverters.asScalaBufferConverter(tx.getListOfOutputs).asScala.zipWithIndex.map(output => {
        convertBitcoinTransactionOutput2Output(output._1, txHashHex, output._2)
      })

      new Transaction2(hash = txHashHex, time = blockTime, block = blockHashHex)
    })
    val block = new Block(hash = blockHashHex, prevHash = prevHash, time = blockTime)
    (block, transactions, inputs.toArray, outputs.toArray)
  }

  def convertBitcoinTransactionOutput2Output(output: BitcoinTransactionOutput, txHash: String, index: Long): Output2 = {
    val address = ConverterUtil.getNiceAddress(output.getTxOutScript)
    new Output2(value = output.getValue, address = address, txRef = TxRef(txHash, index))
  }

  def convertBitcoinTransactionInput2Input(input: BitcoinTransactionInput, txHash: String): Input2 = {
    val address = ConverterUtil.getNiceAddress(input.getTxInScript)
    val prevTxOutIndex: Long = input.getPreviousTxOutIndex
    val prevTransactionHash: String = BitcoinUtil.convertByteArrayToHexString(BitcoinUtil.reverseByteArray(input.getPrevTransactionHash))
    new Input2(value = 0L, address = address, TxRef(prevTransactionHash, prevTxOutIndex), txHash)
  }

  private def printCount(name: String, df: DataFrame) {
    if (debug >= 2) {
      println(s"\n\n$name count: ${df.count}")
    }
  }
}
