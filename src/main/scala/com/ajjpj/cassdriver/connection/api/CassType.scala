package com.ajjpj.cassdriver.connection.api

import akka.util.ByteStringBuilder
import com.ajjpj.cassdriver.connection.protocol_v4.ProtocolV4


sealed trait CassType {
  def serNull(out: ByteStringBuilder) = ProtocolV4.writeInt(out, -1)

  //TODO type coercion
}

object CassTypeInt extends CassType {
  def serNotNull(out: ByteStringBuilder, i: Int) = {
    ProtocolV4.writeInt(out, 4)
    ProtocolV4.writeInt(out, i)
  }
}

object CassTypeString extends CassType {
  def serNotNull(out: ByteStringBuilder, s: String) = ProtocolV4.writeLongString(out, s)
}

object CassTypeBoolean extends CassType {
  def serNotNull(out: ByteStringBuilder, b: Boolean) = {
    ProtocolV4.writeInt(out, 1)
    ProtocolV4.writeByte(out, if(b) 1 else 0)
  }
}