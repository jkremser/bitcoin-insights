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
import org.apache.spark.graphx._
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io._
import org.apache.spark.sql.SparkSession
import org.zuinnote.hadoop.bitcoin.format.common.{BitcoinTransactionOutput, _}
import org.zuinnote.hadoop.bitcoin.format.mapreduce._

object ParquetConverter {
  var debug = 0;

  type TXHashBytes = Array[Byte]
  type TXHashByteArray = ByteArray
  type TXIoIndex = Long
  type TX_ID = (TXHashByteArray, TXIoIndex)
//  type Input = TX_ID
//  type Output = TX_ID
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

    // extract a tuple per transaction containing Bitcoin destination address, the input transaction hash,
    // the input transaction output index, and the current transaction hash, the current transaction output index, a (generated) long identifier
    val bitcoinTransactionTuples: RDD[Transaction] = bitcoinBlocksRDD.flatMap(hadoopKeyValueTuple => extractTransactionData(hadoopKeyValueTuple._2))
//    bitcoinTransactionTuples.cache()

//    // RDD[(BytesWritable, BitcoinBlock)]
//    val foo = bitcoinBlocksRDD.collect()
//    println("\n\n\n\n\n" + foo.size + "\n\n\n\n")
//    foo.foreach(hadoopKeyValueTuple => extractTransactionData(hadoopKeyValueTuple._2))
//    println(foo)

    // create the vertex (Bitcoin destination address, vertexId), keep in mind that the flat table contains the same bitcoin address several times
    val bitcoinAddressIndexed: RDD[(BtcAddress, VertexId)] = bitcoinTransactionTuples.map(bitcoinTransactions => bitcoinTransactions._1).distinct().zipWithIndex()
//    bitcoinAddressIndexed.cache()

    // create the edges (bitcoinAddress,(byteArrayTransaction, TransactionIndex)
    val inputTransactionTuple: RDD[(BtcAddress, Input)] = bitcoinTransactionTuples.map(bitcoinTransactions =>
      (bitcoinTransactions._1, (new ByteArray(bitcoinTransactions._2), bitcoinTransactions._3)))

    // (bitcoinAddress,((byteArrayTransaction, TransactionIndex),vertexId))
    val inputTransactionTupleWithIndex: RDD[(BtcAddress, (Input, VertexId))] = inputTransactionTuple.join(bitcoinAddressIndexed)

    // (byteArrayTransaction, TransactionIndex), (vertexId, bitcoinAddress)
    val inputTransactionTupleByHashIdx: RDD[(Input, VertexId)] = inputTransactionTupleWithIndex.map(iTTuple => (iTTuple._2._1, iTTuple._2._2))

    val currentTransactionTuple: RDD[(BtcAddress, Output)] = bitcoinTransactionTuples.map(bitcoinTransactions =>
      (bitcoinTransactions._1, (new ByteArray(bitcoinTransactions._4), bitcoinTransactions._5)))
    val currentTransactionTupleWithIndex: RDD[(BtcAddress, (Output, VertexId))] = currentTransactionTuple.join(bitcoinAddressIndexed)

    // (byteArrayTransaction, TransactionIndex), (vertexId, bitcoinAddress)
    val currentTransactionTupleByHashIdx: RDD[(Output, VertexId)] = currentTransactionTupleWithIndex.map { cTTuple => (cTTuple._2._1, cTTuple._2._2) }

    // the join creates ((ByteArray, Idx), (srcIdx,srcAddress), (destIdx,destAddress)
    val joinedTransactions: RDD[(TX_ID, (VertexId, VertexId))] = inputTransactionTupleByHashIdx.join(currentTransactionTupleByHashIdx)

    // create vertices => vertexId,bitcoinAddress
    val bitcoinTransactionVertices: RDD[(VertexId, BtcAddress)] = bitcoinAddressIndexed.map { case (k, v) => (v, k) }

    // create edges
    val bitcoinTransactionEdges: RDD[Edge[Int]] = joinedTransactions.map(joinTuple => Edge(joinTuple._2._1, joinTuple._2._2))

    if (debug >= 1) println("\n\n\n\n\n                          ********************saving parquet files****************              \n\n\n\n\n\n\n\n")

    // create two parquet files, one with nodes and second with edges
    val spark = SparkSession
      .builder()
      .getOrCreate()
    import spark.implicits._
    bitcoinTransactionVertices.toDF().write.save(s"$outputDir/nodes")
    bitcoinTransactionEdges.toDF().write.save(s"$outputDir/edges")
  }

  // extract relevant data
  def extractTransactionData(bitcoinBlock: BitcoinBlock): Array[Transaction] = {
    val blockTime: Int = bitcoinBlock.getTime
    val blockHash: Array[Byte] = BitcoinUtil.getBlockHash(bitcoinBlock)
    val blockHashHex: String =  BitcoinUtil.convertByteArrayToHexString(blockHash)
    val rawTransactions: Array[TxData] = bitcoinBlock.getTransactions().toArray(Array[BitcoinTransaction]())

    rawTransactions.map((transaction: BitcoinTransaction) => {
      transaction.getListOfInputs
      val outputs = transaction.getListOfOutputs.toArray(Array[BitcoinTransactionOutput]()).map(output => {
        convertBitcoinTransactionOutput2Output(output)
      })
      val txHash: Array[Byte] = BitcoinUtil.reverseByteArray(BitcoinUtil.getTransactionHash(transaction))
      val txHashHex: String =  BitcoinUtil.convertByteArrayToHexString(txHash)
      new Transaction(id = txHashHex, time = blockTime, block = blockHashHex, inputs = null, outputs = outputs)
    })
  }

  def convertBitcoinTransactionOutput2Output(output: BitcoinTransactionOutput): Output = {
    new Output(value = output.getValue, address = null)
  }

  def convertBitcoinTransactionInput2Input(input: BitcoinTransactionInput): Input = {
    new Input(value = null, address = null)
  }


  // todo: out-txs have the value associated, but input ones don't


}


/**
  * Helper class to make byte arrays comparable
  *
  */
class ByteArray(val bArray: Array[Byte]) extends Serializable {
  override val hashCode = bArray.deep.hashCode

  override def equals(obj: Any) = obj.isInstanceOf[ByteArray] && obj.asInstanceOf[ByteArray].bArray.deep == this.bArray.deep
}

