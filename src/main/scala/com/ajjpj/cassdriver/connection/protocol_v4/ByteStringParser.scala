package com.ajjpj.cassdriver.connection.protocol_v4

import akka.util.ByteString
import com.ajjpj.cassdriver.connection.protocol_v4.ProtocolV4.utf8


class ByteStringParser(in: ByteString, var offs: Int = 0) {
  def readByte(): Byte = {
    val result = in(offs)
    offs += 1
    result
  }

  def readShort(): Int = {
    var result = 0
    result += (readByte() & 0xff) << 8 //NB: shorts are *unsigned* per spec
    result += (readByte() & 0xff)
    result
  }

  def readInt(): Int = {
    var result = 0
    result += (readByte() & 0xff) << 24
    result += (readByte() & 0xff) << 16
    result += (readByte() & 0xff) << 8
    result += (readByte() & 0xff)
    result
  }

  def readShortString(): String = {
    val len = readShort()
    val arr = in.drop(offs).take(len).toArray
    val result = new String (arr, utf8)
    offs += len
    result
  }

  def readString(): String = {
    val len = readInt()
    val arr = in.drop(offs).take(len).toArray
    val result = new String (arr, utf8)
    offs += len
    result
  }
}
