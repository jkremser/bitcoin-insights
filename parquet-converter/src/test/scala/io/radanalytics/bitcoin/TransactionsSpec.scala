/**
*
* This test "unit tests" the application with a local Spark master
*
*/
package io.radanalytics.bitcoin

import java.io.{FileOutputStream, ObjectOutputStream}

import org.scalatest.{BeforeAndAfterAll, FlatSpec, GivenWhenThen, Matchers}
import org.zuinnote.hadoop.bitcoin.format.common.{BitcoinScriptPatternParser, BitcoinTransactionInput, BitcoinUtil}


class TransactionsSpec extends FlatSpec with BeforeAndAfterAll with GivenWhenThen with Matchers  {


override def beforeAll(): Unit = {
    super.beforeAll()

 }

  
  override def afterAll(): Unit = {

    super.afterAll()
}



"Simple Test" should "be always successful" in {
  import java.io.FileInputStream
  import java.io.ObjectInputStream
  val ois = new ObjectInputStream(new FileInputStream("/home/jkremser/foob"))
  val input = ois.readObject.asInstanceOf[BitcoinTransactionInput]

  println("tx: " + input.getPrevTransactionHash)

  println(BitcoinUtil.convertByteArrayToHexString(input.getTxInScript))

//  println("\n\npicinka: " + input.getPrevTransactionHash)
//  println("\n\nkucinka: " + BitcoinScriptPatternParser.getPaymentDestination(input.getPrevTransactionHash))
////  println("\n\nnicinka: " + input.getTxInScriptLength)
////  println("\n\nkacinka: " + BitcoinScriptPatternParser.getPaymentDestination(input.getTxInScriptLength))
//  println("\n\nkonicinka: " + BitcoinUtil.convertByteArrayToHexString(input.getPrevTransactionHash))
//  println("\n\nfocinka: " + BitcoinUtil.convertByteArrayToHexString(BitcoinUtil.reverseByteArray(input.getPrevTransactionHash)))
////  println("\n\npocinka: " + BitcoinUtil.convertByteArrayToHexString(input.getTxInScript))
////  println("\n\nnecinka: " + BitcoinUtil.convertByteArrayToHexString(BitcoinUtil.reverseByteArray(input.getTxInScript)))
}


}
