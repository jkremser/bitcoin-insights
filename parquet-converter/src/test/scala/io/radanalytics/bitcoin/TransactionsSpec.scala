/**
*
* This test "unit tests" the application with a local Spark master
*
*/
package io.radanalytics.bitcoin


import org.scalatest.{BeforeAndAfterAll, FlatSpec, GivenWhenThen, Matchers}
import org.zuinnote.hadoop.bitcoin.format.common.{BitcoinTransactionInput, BitcoinUtil}



class TransactionsSpec extends FlatSpec with BeforeAndAfterAll with GivenWhenThen with Matchers  {


override def beforeAll(): Unit = {
    super.beforeAll()

 }

  
  override def afterAll(): Unit = {

    super.afterAll()
}



"Hash functions and converting them to hex" should "be always successful" in {
  import java.io.FileInputStream
  import java.io.ObjectInputStream
  val source = getClass.getResource("/serialized").getPath
  val ois = new ObjectInputStream(new FileInputStream(source))
  val input = ois.readObject.asInstanceOf[BitcoinTransactionInput]

  assert(BitcoinUtil.convertByteArrayToHexString(input.getPrevTransactionHash) == "9609319FA8102E0506E86E179A142AC4D1B187CD56F508E9F58FC18B0E4DEB11")
  assert(BitcoinUtil.convertByteArrayToHexString(BitcoinUtil.reverseByteArray(input.getPrevTransactionHash)) == "11EB4D0E8BC18FF5E908F556CD87B1D1C42A149A176EE806052E10A89F310996")
  assert(BitcoinUtil.convertByteArrayToHexString(input.getTxInScript) == "493046022100829C046C9D820F6290EB3281E510D1D102AB5BBFD0B0F51DBEF37BA845C237F50221009A43D1F81F2A37AAD357EF0A1243AD66AD9C7F4F223554380AF1921CC41B051601")
  assert(BitcoinUtil.convertByteArrayToHexString(BitcoinUtil.reverseByteArray(input.getTxInScript)) == "0116051BC41C92F10A385435224F7F9CAD66AD43120AEF57D3AA372A1FF8D1439A002102F537C245A87BF3BE1DF5B0D0BF5BAB02D1D110E58132EB90620F829D6C049C82002102463049")
}


}
