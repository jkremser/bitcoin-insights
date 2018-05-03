import sys.process._
import java.io._
import scala.language.postfixOps

val writer = new PrintWriter(new File("hashes.txt"))
val totalBlocks = ("bitcoin-cli getblockcount" !!).trim.toInt
val hashes = (0 to totalBlocks).par.map(hash => s"bitcoin-cli getblockhash $hash" !!)
hashes.foreach(writer.write)
writer.close