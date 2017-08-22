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
import org.apache.hadoop.io._
import org.apache.spark.sql.SparkSession
import org.zuinnote.hadoop.bitcoin.format.common._
import org.zuinnote.hadoop.bitcoin.format.mapreduce._

object ParquetConverter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Bitcoin insights - ParquetConverter)")
    val sc = new SparkContext(conf)
    val hadoopConf = new Configuration();
    convert(sc, hadoopConf, args(0), args(1))
    sc.stop()
  }

  def convert(sc: SparkContext, hadoopConf: Configuration, inputFile: String, outputDir: String): Unit = {
    val bitcoinBlocksRDD = sc.newAPIHadoopFile(inputFile, classOf[BitcoinBlockFileInputFormat], classOf[BytesWritable], classOf[BitcoinBlock], hadoopConf)

    // extract a tuple per transaction containing Bitcoin destination address, the input transaction hash,
    // the input transaction output index, and the current transaction hash, the current transaction output index, a (generated) long identifier
    val bitcoinTransactionTuples = bitcoinBlocksRDD.flatMap(hadoopKeyValueTuple => extractTransactionData(hadoopKeyValueTuple._2))

    // create the vertex (vertexId, Bitcoin destination address), keep in mind that the flat table contains the same bitcoin address several times
    val bitcoinAddressIndexed = bitcoinTransactionTuples.map(bitcoinTransactions => bitcoinTransactions._1).distinct().zipWithIndex()

    // create the edges (bitcoinAddress,(byteArrayTransaction, TransactionIndex)
    val inputTransactionTuple = bitcoinTransactionTuples.map(bitcoinTransactions =>
      (bitcoinTransactions._1, (new ByteArray(bitcoinTransactions._2), bitcoinTransactions._3)))

    // (bitcoinAddress,((byteArrayTransaction, TransactionIndex),vertexId))
    val inputTransactionTupleWithIndex = inputTransactionTuple.join(bitcoinAddressIndexed)

    // (byteArrayTransaction, TransactionIndex), (vertexId, bitcoinAddress)
    val inputTransactionTupleByHashIdx = inputTransactionTupleWithIndex.map(iTTuple => (iTTuple._2._1, (iTTuple._2._2, iTTuple._1)))

    val currentTransactionTuple = bitcoinTransactionTuples.map(bitcoinTransactions =>
      (bitcoinTransactions._1, (new ByteArray(bitcoinTransactions._4), bitcoinTransactions._5)))
    val currentTransactionTupleWithIndex = currentTransactionTuple.join(bitcoinAddressIndexed)

    // (byteArrayTransaction, TransactionIndex), (vertexId, bitcoinAddress)
    val currentTransactionTupleByHashIdx = currentTransactionTupleWithIndex.map { cTTuple => (cTTuple._2._1, (cTTuple._2._2, cTTuple._1)) }

    // the join creates ((ByteArray, Idx), (srcIdx,srcAddress), (destIdx,destAddress)
    val joinedTransactions = inputTransactionTupleByHashIdx.join(currentTransactionTupleByHashIdx)

    // create vertices => vertexId,bitcoinAddress
    val bitcoinTransactionVertices = bitcoinAddressIndexed.map { case (k, v) => (v, k) }

    // create edges
    val bitcoinTransactionEdges = joinedTransactions.map(joinTuple => Edge(joinTuple._2._1._1, joinTuple._2._2._1, "input"))

    // create two parquet files, one with nodes and second with edges
    val spark = SparkSession
      .builder()
      .getOrCreate()
    import spark.implicits._
    bitcoinTransactionVertices.toDF().write.save(s"$outputDir/nodes")
    bitcoinTransactionEdges.toDF().write.save(s"$outputDir/edges")
  }

  // extract relevant data
  def extractTransactionData(bitcoinBlock: BitcoinBlock): Array[(String, Array[Byte], Long, Array[Byte], Long)] = {

    // first we need to determine the size of the result set by calculating the total number of inputs
    // multiplied by the outputs of each transaction in the block
    val transactionCount = bitcoinBlock.getTransactions().size()
    var resultSize = 0
    for (i <- 0 to transactionCount - 1) {
      resultSize += bitcoinBlock.getTransactions().get(i).getListOfInputs().size() * bitcoinBlock.getTransactions().get(i).getListOfOutputs().size()
    }

    // then we can create a tuple for each transaction input: Destination Address (which can be found in the output!), Input Transaction Hash, Current Transaction Hash, Current Transaction Output
    // as you can see there is no 1:1 or 1:n mapping from input to output in the Bitcoin blockchain, but n:m (all inputs are assigned to all outputs), cf. https://en.bitcoin.it/wiki/From_address
    val result: Array[(String, Array[Byte], Long, Array[Byte], Long)] = new Array[(String, Array[Byte], Long, Array[Byte], Long)](resultSize)
    var resultCounter: Int = 0
    for (i <- 0 to transactionCount - 1) { // for each transaction
      val currentTransaction = bitcoinBlock.getTransactions().get(i)
      val currentTransactionHash = BitcoinUtil.getTransactionHash(currentTransaction)
      for (j <- 0 to currentTransaction.getListOfInputs().size() - 1) { // for each input
        val currentTransactionInput = currentTransaction.getListOfInputs().get(j)
        val currentTransactionInputHash = currentTransactionInput.getPrevTransactionHash()
        val currentTransactionInputOutputIndex = currentTransactionInput.getPreviousTxOutIndex()
        for (k <- 0 to currentTransaction.getListOfOutputs().size() - 1) {
          val currentTransactionOutput = currentTransaction.getListOfOutputs().get(k)
          val currentTransactionOutputIndex = k.toLong
          result(resultCounter) = (BitcoinScriptPatternParser.getPaymentDestination(currentTransactionOutput.getTxOutScript()), currentTransactionInputHash, currentTransactionInputOutputIndex, currentTransactionHash, currentTransactionOutputIndex)
          resultCounter += 1
        }
      }

    }
    result;
  }


}


/**
  * Helper class to make byte arrays comparable
  *
  */
class ByteArray(val bArray: Array[Byte]) extends Serializable {
  override val hashCode = bArray.deep.hashCode

  override def equals(obj: Any) = obj.isInstanceOf[ByteArray] && obj.asInstanceOf[ByteArray].bArray.deep == this.bArray.deep
}

