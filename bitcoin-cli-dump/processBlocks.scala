import sys.process._
import java.io._
import scala.language.postfixOps

val blocks = scala.io.Source.fromFile("blockHashes.txt").mkString.split("\n").toList.take(1)
println(blocks)

blocks.foreach(block => {
    val output = Seq("/bin/sh", "-c", s"bitcoin-cli getblock $block | jq -c '.tx'").!!.trim
    val transactions = output.substring(1, output.length - 1).split(",").toList
    transactions.foreach(tx => {
        val txId = tx.substring(1, tx.length - 1)
        // todo: enhance this
        println(s"bitcoin-cli getrawtransaction $txId 1".!!)
    })
})