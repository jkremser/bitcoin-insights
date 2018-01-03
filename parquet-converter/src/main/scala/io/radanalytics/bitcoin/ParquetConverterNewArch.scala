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
import org.apache.spark.sql.SparkSession
import org.zuinnote.hadoop.bitcoin.format.common.{BitcoinTransactionOutput, _}
import org.zuinnote.hadoop.bitcoin.format.mapreduce._

object ParquetConverterNewArch {
  var debug = 0;

  type TXHashBytes = Array[Byte]
  type TXHashByteArray = ByteArray
  type TXIoIndex = Long
  type TXHash = String
  type InputOrder = Long
  type OutputOrder = Long
//  type TX_ID = (TXHashByteArray, TXIoIndex)
  type InputId = (TXHash, InputOrder)
  type OutputId = (TXHash, OutputOrder)
  type BtcAddress = String
  type TxData = BitcoinTransaction
  type VertexId = Long

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Bitcoin insights - ParquetConverter)")
    val sc = new SparkContext(conf)
    val hadoopConf = new Configuration();
    if (args.size == 3) debug = args(2).toInt
    convert(sc, hadoopConf, args(0), args(1))
    sc.stop()
  }

  def convert(sc: SparkContext, hadoopConf: Configuration, inputFile: String, outputDir: String) = {
    if (debug >= 1) println("\n\n\n\n\n                          ********************invoking convert()****************              \n\n\n\n\n\n\n\n")
    val bitcoinBlocksRDD = sc.newAPIHadoopFile(inputFile, classOf[BitcoinBlockFileInputFormat], classOf[BytesWritable], classOf[BitcoinBlock], hadoopConf)

    val bitcoinTransactions: RDD[Transaction] = bitcoinBlocksRDD.flatMap(hadoopKeyValueTuple => extractTransactionData(hadoopKeyValueTuple._2))
//    bitcoinTransactions.cache()

    if (debug >= 2) {
      println("\n\n\n\n\n                          ********************TX****************              \n\n\n\n\n\n\n\n")
      val transactions = bitcoinTransactions.collect()
      println("\n\n\n\n\ntransactions.size = " + transactions.size + "\n\n\n\ntransactions:")
      transactions.foreach(println)
    }

    val allInputs: RDD[Input] = bitcoinTransactions.flatMap(tx => tx.inputs)
    val allOutputs: RDD[Output] = bitcoinTransactions.flatMap(tx => tx.outputs)

    val inputsIndexed: RDD[((InputId), Input)] = allInputs.map(input => (input.prevTx, input))
    val outputsIndexed: RDD[((OutputId), Output)] = allOutputs.map(output => (output.nextTx, output))

    val joined: RDD[(Input, Output)] = inputsIndexed.join(outputsIndexed).values

    val cajk: RDD[(Input, Output)] = joined.map(tuple => {
      tuple._1.address = tuple._2.address
      tuple
    })

    val addresses: RDD[String] = allOutputs.map(_.address).distinct()

    // todo create edges - 2 types (tx - address), nodes - 2 types

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
    var i = 30
    rawTransactions.map((transaction: BitcoinTransaction) => {
      val txHash: Array[Byte] = BitcoinUtil.reverseByteArray(BitcoinUtil.getTransactionHash(transaction))
      val txHashHex: String =  BitcoinUtil.convertByteArrayToHexString(txHash)
      val outputs = transaction.getListOfOutputs.toArray(Array[BitcoinTransactionOutput]()).zipWithIndex.map(output => {
        convertBitcoinTransactionOutput2Output(output._1, txHashHex, output._2)
      })

      val inputs = transaction.getListOfInputs.toArray(Array[BitcoinTransactionInput]()).map(input => {
        i = i - 1
        convertBitcoinTransactionInput2Input(input)
      })
      new Transaction(id = txHashHex, time = blockTime, block = blockHashHex, inputs = inputs, outputs = outputs)
    })
  }

  def getNiceAddress(ugly: Array[Byte]): String = {
    val niceAddress = Option(BitcoinScriptPatternParser.getPaymentDestination(ugly))
    val niceAddressStripped = niceAddress.map(_.stripPrefix("bitcoinaddress_"))
    niceAddressStripped.getOrElse("unknown")
  }

  def convertBitcoinTransactionOutput2Output(output: BitcoinTransactionOutput, txHash: String, index: Long): Output = {
    val address = getNiceAddress(output.getTxOutScript)
    new Output(value = output.getValue, address = address, nextTx = (txHash, index))
  }

  def convertBitcoinTransactionInput2Input(input: BitcoinTransactionInput): Input = {
    val address = getNiceAddress(input.getTxInScript)
    // todo: out-txs have the value associated, but input ones don't
    val prevTxOutIndex: Long = input.getPreviousTxOutIndex
    val prevTransactionHash: String = BitcoinUtil.convertByteArrayToHexString(BitcoinUtil.reverseByteArray(input.getPrevTransactionHash))
    new Input(value = 0L, address = address, (prevTransactionHash, prevTxOutIndex))
  }
}
