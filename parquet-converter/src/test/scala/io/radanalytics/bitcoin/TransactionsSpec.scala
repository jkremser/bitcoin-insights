/**
  *
  * This test "unit tests" the BitcoinTransactionInput object
  *
  */
package io.radanalytics.bitcoin


import org.scalatest.{BeforeAndAfterAll, FlatSpec, GivenWhenThen, Matchers}
import org.zuinnote.hadoop.bitcoin.format.common.{BitcoinTransactionInput, BitcoinUtil}


class TransactionsSpec extends FlatSpec with BeforeAndAfterAll with GivenWhenThen with Matchers {

  var input: BitcoinTransactionInput = null;


  override def beforeAll(): Unit = {
    super.beforeAll()
    import java.io.FileInputStream
    import java.io.ObjectInputStream
    val source = getClass.getResource("/serialized").getPath
    val ois = new ObjectInputStream(new FileInputStream(source))
    input = ois.readObject.asInstanceOf[BitcoinTransactionInput]
  }


  override def afterAll(): Unit = {
    super.afterAll()
    input = null;
  }


  "Prev hash of giver input in hex" should "be 9609319FA.." in {
    assert(BitcoinUtil.convertByteArrayToHexString(input.getPrevTransactionHash) == "9609319FA8102E0506E86E179A142AC4D1B187CD56F508E9F58FC18B0E4DEB11")
  }

  "Prev hash of giver input in hex with reversed endianness" should "be 11EB4D0E8BC.." in {
    assert(BitcoinUtil.convertByteArrayToHexString(BitcoinUtil.reverseByteArray(input.getPrevTransactionHash)) == "11EB4D0E8BC18FF5E908F556CD87B1D1C42A149A176EE806052E10A89F310996")
  }

  "Input hashed in hex" should "be 49304602.." in {
    assert(BitcoinUtil.convertByteArrayToHexString(input.getTxInScript) == "493046022100829C046C9D820F6290EB3281E510D1D102AB5BBFD0B0F51DBEF37BA845C237F50221009A43D1F81F2A37AAD357EF0A1243AD66AD9C7F4F223554380AF1921CC41B051601")
  }

  "Input hashed in hex with reversed endianness" should "be 0116051BC4.." in {
    assert(BitcoinUtil.convertByteArrayToHexString(BitcoinUtil.reverseByteArray(input.getTxInScript)) == "0116051BC41C92F10A385435224F7F9CAD66AD43120AEF57D3AA372A1FF8D1439A002102F537C245A87BF3BE1DF5B0D0BF5BAB02D1D110E58132EB90620F829D6C049C82002102463049")
  }

}
