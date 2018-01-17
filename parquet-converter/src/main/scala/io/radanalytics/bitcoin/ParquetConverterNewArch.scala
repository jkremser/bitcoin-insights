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

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.conf._
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.zuinnote.hadoop.bitcoin.format.common._
import org.zuinnote.hadoop.bitcoin.format.mapreduce.BitcoinBlockFileInputFormat

import scala.collection.JavaConverters
import scala.collection.mutable.MutableList

object ParquetConverterNewArch {
  var debug = 0

  import CustomTypes._

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Bitcoin insights - ParquetConverter)")
    val sc = new SparkContext(conf)
    val hadoopConf = new Configuration()
    if (args.size == 3) debug = args(2).toInt
    convert(sc, hadoopConf, args(0), args(1))
    sc.stop()
  }

  def convert(sc: SparkContext, hadoopConf: Configuration, inputFile: String, outputDir: String) = {
    if (debug >= 1) println("\n\n\n\n\n                          ********************invoking convert()****************              \n\n\n\n\n\n\n\n")
    val bitcoinBlocksRDD = sc.newAPIHadoopFile(inputFile, classOf[BitcoinBlockFileInputFormat], classOf[BytesWritable], classOf[BitcoinBlock], hadoopConf)

    val blockchainData: RDD[(Block, Array[Transaction2], Array[Input], Array[Output])] = bitcoinBlocksRDD.map(hadoopKeyValueTuple => extractData(hadoopKeyValueTuple._2))
    val allInputs: RDD[Input] = blockchainData.flatMap(t => t._3)
    val allOutputs: RDD[Output] = blockchainData.flatMap(t => t._4)

    if (debug >= 2) {
      println("\n\n\n\n\n allOutputs: RDD[Output] = \n\n")
      allOutputs.sample(true, 0.001).take(5).foreach(println)
    }

    val indexedInputs: RDD[((IO_REF), Input)] = allInputs.map(in => (in.txOutputRef, in))
    val indexedOutputs: RDD[((IO_REF), Output)] = allOutputs.map(out => (out.txRef, out))

    // match inputs to outputs => output that doesn't have corresponding input is considered as unspent and the sum of
    // its values per address represents the balance
    val joined: RDD[(Output, Option[Input])] = indexedOutputs.leftOuterJoin(indexedInputs).values

    if (debug >= 1) {
      println("\n\n\n\n\n joinedFixed: RDD[(Output, Input)] = \n\n")
      joined.sample(true, 0.001).take(5).foreach(println)
    }

    // note: distinct shuffles
    val addresses: RDD[String] = allOutputs.map(_.address).distinct()
    if (debug >= 2) {
      val strange = addresses.filter(a => a.contains("_"))
      println("\n\n\n\n\n\nNON-STANDARD #: " + strange.count())
      println("\n\n\n\n\n\nNON-STANDARD (bitcoinpubkey_)#: " + strange.filter(s => s.contains("bitcoinpubkey")).count())
      println("\n\n\n\n\n\nNON-STANDARD  :" + strange.sample(true, 0.1).take(5).foreach(println))
//      other possible prefixes: "puzzle_" "anyone" "unspendable"
    }

    if (debug >= 1) {
      println("\n\n\n\n\n addresses: RDD[String] = \n\n")
      addresses.sample(true, 0.001).take(5).foreach(println)
    }

    val allTransactions: RDD[Transaction2] = blockchainData.flatMap(t => t._2)
    val allBlocks: RDD[Block] = blockchainData.map(t => t._1)

    // create two parquet files, one with nodes and second with edges
    val spark = SparkSession
      .builder()
      .getOrCreate()
    import spark.implicits._

    val addressesDF: DataFrame = addresses.toDF("address")
    addressesDF.write.mode("overwrite").format("parquet").option("compression", "gzip").mode("overwrite").save(s"$outputDir/addresses")

    val allBlocksDF = allBlocks.map(b => (b.hash, b.time)).toDF("hash", "time")
    allBlocksDF.write.mode("overwrite").format("parquet").option("compression", "gzip").mode("overwrite").save(s"$outputDir/blocks")

    val allTransactionsDF = allTransactions.map(t => (t.hash)).toDF("hash")
    allTransactionsDF.write.mode("overwrite").format("parquet").option("compression", "gzip").mode("overwrite").save(s"$outputDir/transactions")

    if (debug >= 2) {
      println("\n\n\n\n\n allOutputs.map(o => (o.txRef._1, o.address, o.value)) = \n\n")
      allOutputs.map(o => (o.txRef._1, o.address, o.value)).sample(true, 0.001).take(10).foreach(println)
    }

    val allEdgesDF = allTransactions.map(t => (t.block, t.hash, 0L)) // block -> TX
        .union(joined.flatMap(io => if (io._2.isDefined) Array((io._1.address, io._2.get.tx, io._1.value)) else Array[(String, String, Long)]())) // address => TX
        .union(allOutputs.map(o => (o.txRef._1, o.address, o.value))) // TX -> address
        .union(allBlocks.map(b => (b.hash, b.prevHash, 0L))) // block -> previous block
        .toDF("src", "dst", "value")

    allEdgesDF.write.mode("overwrite").format("parquet").option("compression", "gzip").mode("overwrite").save(s"$outputDir/edges")
  }

  def extractData(bitcoinBlock: BitcoinBlock): (Block, Array[Transaction2], Array[Input], Array[Output]) = {
    val blockTime: Int = bitcoinBlock.getTime
    val blockHashHex: String = BitcoinUtil.convertByteArrayToHexString(BitcoinUtil.getBlockHash(bitcoinBlock))
    val prevHash: String = BitcoinUtil.convertByteArrayToHexString(BitcoinUtil.reverseByteArray(bitcoinBlock.getHashPrevBlock))
    val rawTransactions: Array[TxData] = bitcoinBlock.getTransactions().toArray(Array[BitcoinTransaction]())
    val inputs: MutableList[Input] = MutableList[Input]()
    val outputs: MutableList[Output] = MutableList[Output]()

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



  def convertBitcoinTransactionOutput2Output(output: BitcoinTransactionOutput, txHash: String, index: Long): Output = {
    val address = ConverterUtil.getNiceAddress(output.getTxOutScript)
    new Output(value = output.getValue, address = address, txRef = (txHash, index))
  }

  def convertBitcoinTransactionInput2Input(input: BitcoinTransactionInput, txHash: String): Input = {
    val address = ConverterUtil.getNiceAddress(input.getTxInScript)
    val prevTxOutIndex: Long = input.getPreviousTxOutIndex
    val prevTransactionHash: String = BitcoinUtil.convertByteArrayToHexString(BitcoinUtil.reverseByteArray(input.getPrevTransactionHash))
    new Input(value = 0L, address = address, (prevTransactionHash, prevTxOutIndex), txHash)
  }
}
