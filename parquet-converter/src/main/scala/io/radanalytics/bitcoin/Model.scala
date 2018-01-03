package io.radanalytics.bitcoin

class Input(val value: Long, val address: String, val prevTx: (String, Long)) extends Serializable {
  override def toString: String = {
    s" - val=$value ; adr=$address\n"
  }
}

class Output(val value: Long, val address: String, val nextTx: (String, Long)) extends Serializable {
  override def toString: String = {
    s" - val=$value ; adr=$address\n"
  }
}

class Transaction(val id: String, val time: Int, val block: String, val inputs: Array[Input], val outputs: Array[Output]) extends Serializable {
  override def toString: String = {
    s"txId:    $id\ntime:    ${new java.util.Date(time * 1000L).toString}\nblock:   $block\n\ninputs:\n${inputs.foldLeft("")(_+_)}\noutputs:\n${outputs.foldLeft("")(_+_)}\n----------\n"
  }
}
