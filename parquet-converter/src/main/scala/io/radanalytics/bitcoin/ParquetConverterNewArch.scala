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

import io.radanalytics.bitcoin.ParquetConverter.debug
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.conf._
import org.apache.spark.graphx._
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.zuinnote.hadoop.bitcoin.format.common.{BitcoinTransactionOutput, _}
import org.zuinnote.hadoop.bitcoin.format.mapreduce._

import scala.collection.{JavaConversions, JavaConverters}
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

    //    val bitcoinTransactions: RDD[Transaction] = bitcoinBlocksRDD.flatMap(hadoopKeyValueTuple => extractTransactionData(hadoopKeyValueTuple._2))


    val blockchainData: RDD[(Block, Array[Transaction2], Array[Input], Array[Output])] = bitcoinBlocksRDD.map(hadoopKeyValueTuple => extractData(hadoopKeyValueTuple._2))
    val allInputs: RDD[Input] = blockchainData.flatMap(t => t._3)
    val allOutputs: RDD[Output] = blockchainData.flatMap(t => t._4)

    val indexedInputs: RDD[((IO_REF), Input)] = allInputs.map(in => (in.txOutputRef, in))
    val indexedOutputs: RDD[((IO_REF), Output)] = allOutputs.map(out => (out.txRef, out))

    // match inputs to outputs => output that doesn't have corresponding input is considered as unspent and the sum of
    // its values per address represents the balance
    val joined: RDD[(Output, Option[Input])] = indexedOutputs.leftOuterJoin(indexedInputs).values

//    val joinedFixed: RDD[(Output, Input)] = joined.map(tuple => {
//      tuple._2.address = tuple._1.address
//      tuple._2.value = tuple._1.value
//      tuple
//    })

    if (debug >= 1) {
      println("\n\n\n\n\n joinedFixed: RDD[(Output, Input)] = \n\n")
      joined.sample(true, 0.001).take(5).foreach(println)
    }

    val addresses: RDD[String] = allOutputs.map(_.address).distinct()

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
    addressesDF.write.save(s"$outputDir/addresses")

    val allBlocksDF = allBlocks.map(b => (b.hash, b.time)).toDF("hash", "time")
    allBlocksDF.write.save(s"$outputDir/blocks")

    val allTransactionsDF = allTransactions.map(t => (t.hash)).toDF("hash")
    allTransactionsDF.write.save(s"$outputDir/transactions")

    val allEdgesDF = allTransactions.map(t => (t.block, t.hash, 0L)) // blocks -> TXs
        .union(joined.flatMap(io => if (io._2.isDefined) Array((io._1.address, io._2.get.tx, io._1.value)) else Array[(String, String, Long)]()))
        .union(allOutputs.map(o => (o.txRef._1, o.address, o.value)))
        .toDF("src", "dst", "value")

    allEdgesDF.write.save(s"$outputDir/edges")

//    bitcoinTransactionEdges.toDF().write.save(s"$outputDir/edges")

//    val allTransactionsDF = allTransactions.map


    // todo create edges - 2 types (tx - address), nodes - 2 types

//    allOutputs.sample(true, 0.001).take(5).foreach(println)


//    bitcoinTransactions.cache()

//    if (debug >= 2) {
//      println("\n\n\n\n\n                          ********************TX****************              \n\n\n\n\n\n\n\n")
//      val transactions = bitcoinTransactions.collect()
//      println("\n\n\n\n\ntransactions.size = " + transactions.size + "\n\n\n\ntransactions:")
//      transactions.foreach(println)
//    }
//
//    val allInputs: RDD[Input] = bitcoinTransactions.flatMap(tx => tx.inputs)
//    val allOutputs: RDD[Output] = bitcoinTransactions.flatMap(tx => tx.outputs)
//
//    val inputsIndexed: RDD[((InputId), Input)] = allInputs.map(input => (input.prevTx, input))
//    val outputsIndexed: RDD[((OutputId), Output)] = allOutputs.map(output => (output.nextTx, output))
//
//    val joined: RDD[(Input, Output)] = inputsIndexed.join(outputsIndexed).values
//
//    val cajk: RDD[(Input, Output)] = joined.map(tuple => {
//      tuple._1.address = tuple._2.address
//      tuple
//    })
//
//    cajk.take(5).foreach(io => println(s"--->\ntx:${io._1.prevTx._1}\ninput: ${io._1}\n output: ${io._2}\n--<"))
//
//    val addresses: RDD[String] = allOutputs.map(_.address).distinct()
//
//    // todo create edges - 2 types (tx - address), nodes - 2 types

//    // create the vertex (Bitcoin destination address, vertexId), keep in mind that the flat table contains the same bitcoin address several times
//    val bitcoinAddressIndexed: RDD[(BtcAddress, VertexId)] = bitcoinTransactionTuples.map(bitcoinTransactions => bitcoinTransactions._1).distinct().zipWithIndex()
//
//    // create the edges (bitcoinAddress,(byteArrayTransaction, TransactionIndex)
//    val inputTransactionTuple: RDD[(BtcAddress, Input)] = bitcoinTransactionTuples.map(bitcoinTransactions =>
//      (bitcoinTransactions._1, (new ByteArray(bitcoinTransactions._2), bitcoinTransactions._3)))
//
//    // (bitcoinAddress,((byteArrayTransaction, TransactionIndex),vertexId))
//    val inputTransactionTupleWithIndex: RDD[(BtcAddress, (Input, VertexId))] = inputTransactionTuple.join(bitcoinAddressIndexed)
//
//    // (byteArrayTransaction, TransactionIndex), (vertexId, bitcoinAddress)
//    val inputTransactionTupleByHashIdx: RDD[(Input, VertexId)] = inputTransactionTupleWithIndex.map(iTTuple => (iTTuple._2._1, iTTuple._2._2))
//
//    val currentTransactionTuple: RDD[(BtcAddress, Output)] = bitcoinTransactionTuples.map(bitcoinTransactions =>
//      (bitcoinTransactions._1, (new ByteArray(bitcoinTransactions._4), bitcoinTransactions._5)))
//    val currentTransactionTupleWithIndex: RDD[(BtcAddress, (Output, VertexId))] = currentTransactionTuple.join(bitcoinAddressIndexed)
//
//    // (byteArrayTransaction, TransactionIndex), (vertexId, bitcoinAddress)
//    val currentTransactionTupleByHashIdx: RDD[(Output, VertexId)] = currentTransactionTupleWithIndex.map { cTTuple => (cTTuple._2._1, cTTuple._2._2) }
//
//    // the join creates ((ByteArray, Idx), (srcIdx,srcAddress), (destIdx,destAddress)
//    val joinedTransactions: RDD[(TX_ID, (VertexId, VertexId))] = inputTransactionTupleByHashIdx.join(currentTransactionTupleByHashIdx)
//
//    // create vertices => vertexId,bitcoinAddress
//    val bitcoinTransactionVertices: RDD[(VertexId, BtcAddress)] = bitcoinAddressIndexed.map { case (k, v) => (v, k) }
//
//    // create edges
//    val bitcoinTransactionEdges: RDD[Edge[Int]] = joinedTransactions.map(joinTuple => Edge(joinTuple._2._1, joinTuple._2._2))
//
//    if (debug >= 1) println("\n\n\n\n\n                          ********************saving parquet files****************              \n\n\n\n\n\n\n\n")
//
//    // create two parquet files, one with nodes and second with edges
//    val spark = SparkSession
//      .builder()
//      .getOrCreate()
//    import spark.implicits._
//    bitcoinTransactionVertices.toDF().write.save(s"$outputDir/nodes")
//    bitcoinTransactionEdges.toDF().write.save(s"$outputDir/edges")
  }

  // extract relevant data
  def extractTransactionData(bitcoinBlock: BitcoinBlock): Array[Transaction] = {
    val blockTime: Int = bitcoinBlock.getTime
    val blockHash: Array[Byte] = BitcoinUtil.getBlockHash(bitcoinBlock)
    val blockHashHex: String =  BitcoinUtil.convertByteArrayToHexString(blockHash)
    val rawTransactions: Array[TxData] = bitcoinBlock.getTransactions().toArray(Array[BitcoinTransaction]())
    rawTransactions.map((transaction: BitcoinTransaction) => {
      val txHash: Array[Byte] = BitcoinUtil.reverseByteArray(BitcoinUtil.getTransactionHash(transaction))
      val txHashHex: String =  BitcoinUtil.convertByteArrayToHexString(txHash)
      val outputs = transaction.getListOfOutputs.toArray(Array[BitcoinTransactionOutput]()).zipWithIndex.map(output => {
        convertBitcoinTransactionOutput2Output(output._1, txHashHex, output._2)
      })

      val inputs = transaction.getListOfInputs.toArray(Array[BitcoinTransactionInput]()).map(input => {
        convertBitcoinTransactionInput2Input(input, txHashHex)
      })
      new Transaction(hash = txHashHex, time = blockTime, block = blockHashHex, inputs = inputs, outputs = outputs)
    })
  }


  def extractData(bitcoinBlock: BitcoinBlock): (Block, Array[Transaction2], Array[Input], Array[Output]) = {
    val blockTime: Int = bitcoinBlock.getTime
    val blockHashHex: String = BitcoinUtil.convertByteArrayToHexString(BitcoinUtil.getBlockHash(bitcoinBlock))
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
    val block = new Block(hash = blockHashHex, time = blockTime)
    (block, transactions, inputs.toArray, outputs.toArray)
  }

  def getNiceAddress(ugly: Array[Byte]): String = {
    val niceAddress = Option(BitcoinScriptPatternParser.getPaymentDestination(ugly))
    val niceAddressStripped = niceAddress.map(_.stripPrefix("bitcoinaddress_"))
    niceAddressStripped.getOrElse("unknown")
  }

  def convertBitcoinTransactionOutput2Output(output: BitcoinTransactionOutput, txHash: String, index: Long): Output = {
    val address = getNiceAddress(output.getTxOutScript)
    new Output(value = output.getValue, address = address, txRef = (txHash, index))
  }

  def convertBitcoinTransactionInput2Input(input: BitcoinTransactionInput, txHash: String): Input = {
    val address = getNiceAddress(input.getTxInScript)
    // todo: out-txs have the value associated, but input ones don't
    val prevTxOutIndex: Long = input.getPreviousTxOutIndex
    val prevTransactionHash: String = BitcoinUtil.convertByteArrayToHexString(BitcoinUtil.reverseByteArray(input.getPrevTransactionHash))
    new Input(value = 0L, address = address, (prevTransactionHash, prevTxOutIndex), txHash)
  }
}
