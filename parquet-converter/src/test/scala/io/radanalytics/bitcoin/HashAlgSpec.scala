/**
  *
  * This test "unit tests" the BitcoinTransactionInput object
  *
  */
package io.radanalytics.bitcoin

import org.scalatest.{BeforeAndAfterAll, FlatSpec, GivenWhenThen, Matchers}
import org.zuinnote.hadoop.bitcoin.format.common.{BitcoinTransactionInput, BitcoinUtil}


class HashAlgSpec extends FlatSpec with BeforeAndAfterAll with GivenWhenThen with Matchers {


  "ConverterUtil.sha256 applied on 0430DB17C4.." should "return D6A739B77.." in {
    val inputHex = "0430DB17C4AE5A12418F911284F35DFDB814DA33434202B9735BC42CF171F468C01E386B827FC683C3B750CAD7522692A942C30529E14D80C523A89BF880DBCFD5"
    val desiredOutputHex = "D6A739B77ED9E701D1FCFBD2E4589BFF6B38C651125E3DBEE7CC9FCB1A6CB3C5"
    assert(BitcoinUtil.convertByteArrayToHexString(ConverterUtil.sha256(BitcoinUtil.convertHexStringToByteArray(inputHex))) == desiredOutputHex)
  }

  "ConverterUtil.ripemd160 applied on 0430DB17C4.." should "return 4861455.." in {
    val inputHex = "0430DB17C4AE5A12418F911284F35DFDB814DA33434202B9735BC42CF171F468C01E386B827FC683C3B750CAD7522692A942C30529E14D80C523A89BF880DBCFD5"
    val desiredOutputHex = "48614559893DF9816B8795B9F41C8C903EA7F332"
    assert(BitcoinUtil.convertByteArrayToHexString(ConverterUtil.ripemd160(BitcoinUtil.convertHexStringToByteArray(inputHex))) == desiredOutputHex)
  }
}