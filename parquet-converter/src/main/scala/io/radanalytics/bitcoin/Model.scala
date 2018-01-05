package io.radanalytics.bitcoin

import org.zuinnote.hadoop.bitcoin.format.common.BitcoinTransaction

object CustomTypes {
  type VertexId = Long
  type IO_REF = (String, Long)
  type TxData = BitcoinTransaction
}
import CustomTypes._


class Input(var value: Long, var address: String, val txOutputRef: IO_REF, val tx: String) extends Serializable {
  override def toString: String = {
    s" - val=$value ; adr=$address\n   txOutputRef=$txOutputRef; tx=$tx"
  }
}

class Output(val value: Long, val address: String, val txRef: IO_REF) extends Serializable {
  override def toString: String = {
    s" - val=$value ; adr=$address\n   txRef=$txRef"
  }
}

class Transaction(val hash: String, val time: Int, val block: String, val inputs: Array[Input], val outputs: Array[Output]) extends Serializable {
  override def toString: String = {
    s"txId:    $hash\ntime:    ${new java.util.Date(time * 1000L).toString}\nblock:   $block\n\ninputs:\n${inputs.foldLeft("")(_+_)}\noutputs:\n${outputs.foldLeft("")(_+_)}\n----------\n"
  }
}

class Transaction2(val hash: String, val time: Int, val block: String) extends Serializable {
  override def toString: String = {
    s"txId:    $hash\ntime:    ${new java.util.Date(time * 1000L).toString}\nblock:   $block\n"
  }
}

class Block(val hash: String, val time: Int) extends Serializable {
  override def toString: String = {
    s"block id:    $hash\ntime:    ${new java.util.Date(time * 1000L).toString}\n"
  }
}
