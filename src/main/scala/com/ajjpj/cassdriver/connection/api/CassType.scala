package com.ajjpj.cassdriver.connection.api

import com.ajjpj.cassdriver.util.ByteBuffersBuilder


sealed trait CassType {
  def serNull(out: ByteBuffersBuilder) = out.writeInt(-1)

  //TODO type coercion
}

object CassTypeInt extends CassType {
  def serNotNull(out: ByteBuffersBuilder, i: Int) = {
    out.writeInt(4)
    out.writeInt(i)
  }
}

object CassTypeString extends CassType {
  def serNotNull(out: ByteBuffersBuilder, s: String) = out.writeLongString(s)
}

object CassTypeBoolean extends CassType {
  def serNotNull(out: ByteBuffersBuilder, b: Boolean) = {
    out.writeInt(1)
    out.writeByte(if(b) 1 else 0)
  }
}