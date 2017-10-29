package com.ajjpj.cassdriver.connection.metadata

import com.ajjpj.cassdriver.util.{ByteBuffersBuilder, ParsableByteBuffers}


/**
  * see section 6 of the spec
  */
sealed trait CassType {
  def serNull(out: ByteBuffersBuilder) = out.writeInt(-1)

  def deser(in: ParsableByteBuffers) = {
    val n = in.readInt()
    require(n >= -2)
    n match {
      case _ if n == -1 => null
      case _ if n == -2 => ???
      case _ if n == 0 => defaultValue
      case _ if n > 0 => deserNotNull(in, n)
    }
  }

  def defaultValue: Any
  def deserNotNull(in: ParsableByteBuffers, n: Int): Any

  //TODO type coercion
}
object CassType {
  def read(frame: ParsableByteBuffers): CassType = frame.readUnsignedShort() match {
    case 0x0004 => CassTypeBoolean
    case 0x0009 => CassTypeInt
    case 0x000d => CassTypeString
      //TODO add missing types
      //TODO move to type registry?
  }
}

object CassTypeInt extends CassType {
  def serNotNull(out: ByteBuffersBuilder, i: Int) = {
    out.writeInt(4)
    out.writeInt(i)
  }

  override def defaultValue = 0
  override def deserNotNull (in: ParsableByteBuffers, n: Int) = {
    require (n == 4)
    in.readInt()
  }
}

object CassTypeString extends CassType {
  def serNotNull(out: ByteBuffersBuilder, s: String) = out.writeLongString(s)

  override def defaultValue = "" //TODO verify this
  override def deserNotNull (in: ParsableByteBuffers, n: Int) = {
    in.stringRaw(n)
  }
}

object CassTypeBoolean extends CassType {
  def serNotNull(out: ByteBuffersBuilder, b: Boolean) = {
    out.writeInt(1)
    out.writeByte(if(b) 1 else 0)
  }

  override def defaultValue = false
  override def deserNotNull (in: ParsableByteBuffers, n: Int) = {
    require (n == 1)
    in.readByte() != 0
  }
}