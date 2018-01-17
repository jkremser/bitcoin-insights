package io.radanalytics.bitcoin

import java.security.{MessageDigest, NoSuchAlgorithmException}
import java.util

import org.bouncycastle.crypto.digests.{RIPEMD160Digest, SHA256Digest}
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinUtil

class ConverterUtil extends Serializable

object ConverterUtil {

  def sha256(input: Array[Byte]): Array[Byte] = {
    val d = new SHA256Digest
    d.update(input, 0, input.length)
    val result = new Array[Byte](d.getDigestSize)
    d.doFinal(result, 0)
    result
  }

  def ripemd160(input: Array[Byte]): Array[Byte] = {
    try {
      val sha256 = MessageDigest.getInstance("SHA-256").digest(input)
      val digest = new RIPEMD160Digest
      digest.update(sha256, 0, sha256.length)
      val out = new Array[Byte](20)
      digest.doFinal(out, 0)
      return out
    } catch {
      case e: NoSuchAlgorithmException =>
        throw new RuntimeException(e) // Cannot happen.
    }
  }


  // ugly Java code that was copy pasted and altered
  def getPaymentDestination(scriptPubKey: Array[Byte]): String = {
    if (scriptPubKey == null) return null
    // test if anyone can spend output
    if (scriptPubKey.length == 0) return "anyone" // need to check also ScriptSig for OP_TRUE
    // test if standard transaction to Bitcoin address
    val payToHash: String = checkPayToHash(scriptPubKey)
    if (payToHash != null) return payToHash
    // test if obsolete transaction to public key
    val payToPubKey: String = checkPayToPubKey(scriptPubKey)
    if (payToPubKey != null) return payToPubKey
    // test if puzzle
    if ((scriptPubKey.length > 0) && ((scriptPubKey(0) & 0xFF) == 0xAA) && ((scriptPubKey(scriptPubKey.length - 1) & 0xFF) == 0x87)) {
      val puzzle: Array[Byte] = util.Arrays.copyOfRange(scriptPubKey, 1, scriptPubKey.length - 2)
      return "puzzle_" + BitcoinUtil.convertByteArrayToHexString(puzzle)
    }
    // test if unspendable
    if ((scriptPubKey.length > 0) && ((scriptPubKey(0) & 0xFF) == 0x6a)) return "unspendable"
    null
  }

  private def checkPayToHash(scriptPubKey: Array[Byte]): String = { // test start
    val validLength = scriptPubKey.length == 25
    if (!validLength) return null
    val validStart = ((scriptPubKey(0) & 0xFF) == 0x76) && ((scriptPubKey(1) & 0xFF) == 0xA9) && ((scriptPubKey(2) & 0xFF) == 0x14)
    val validEnd = ((scriptPubKey(23) & 0xFF) == 0x88) && ((scriptPubKey(24) & 0xFF) == 0xAC)
    if (validStart && validEnd) {
      val bitcoinAddress = util.Arrays.copyOfRange(scriptPubKey, 3, 23)
      return BitcoinUtil.convertByteArrayToHexString(bitcoinAddress)
    }
    null
  }

  private def checkPayToPubKey(scriptPubKey: Array[Byte]): String = {
    if ((scriptPubKey.length > 0) && ((scriptPubKey(scriptPubKey.length - 1) & 0xFF) == 0xAC)) {
      return BitcoinUtil.convertByteArrayToHexString(ripemd160(util.Arrays.copyOfRange(scriptPubKey, 1, scriptPubKey.length - 1)))
    }
    null
  }


  def getNiceAddress(ugly: Array[Byte]): String = {
    val niceAddress = Option(ConverterUtil.getPaymentDestination(ugly))
//    val niceAddressStripped = niceAddress.map(_.stripPrefix("bitcoinaddress_"))
    niceAddress.getOrElse("unknown")
  }
}

