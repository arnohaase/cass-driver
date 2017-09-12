package com.ajjpj.cassdriver.connection.protocol_v4

import java.nio.charset.Charset

import akka.util.ByteString


private[protocol_v4] class CassandraFrameBuilder (flags: MessageFlags, stream: Int, opCode: Int) {
  import CassandraFrameBuilder._

  // ---- buffer handling, primitives

  private var buffer = Array.ofDim[Byte](256)
  private var offs = 0

  private def ensureCapacity(numBytes: Int): Unit = {
    if(buffer.length - offs < numBytes) {
      val newBuf = Array.ofDim[Byte](Math.max(2*buffer.length, buffer.length + numBytes))
      System.arraycopy(buffer, 0, newBuf, 0, offs)
      buffer = newBuf
    }
  }

  def short(s: Int) = {
    require (s >= 0, s"short must be non-negative: $s")
    require (s <= 0xffff, s"short bigger than 0xffff: $s")
    ensureCapacity (2)
    buffer(offs)   = (s >> 8).asInstanceOf[Byte]
    buffer(offs+1) =  s      .asInstanceOf[Byte]
    offs += 2
  }
  def int(i: Int) = {
    ensureCapacity(4)
    buffer(offs)   = (i >> 24).asInstanceOf[Byte]
    buffer(offs+1) = (i >> 16).asInstanceOf[Byte]
    buffer(offs+2) = (i >>  8).asInstanceOf[Byte]
    buffer(offs+3) =  i       .asInstanceOf[Byte]
    offs += 4
  }
  def long(l: Long) = {
    ensureCapacity(8)
    buffer(offs)   = (l >> 56).asInstanceOf[Byte]
    buffer(offs+1) = (l >> 48).asInstanceOf[Byte]
    buffer(offs+2) = (l >> 40).asInstanceOf[Byte]
    buffer(offs+3) = (l >> 32).asInstanceOf[Byte]
    buffer(offs+4) = (l >> 24).asInstanceOf[Byte]
    buffer(offs+5) = (l >> 16).asInstanceOf[Byte]
    buffer(offs+6) = (l >>  8).asInstanceOf[Byte]
    buffer(offs+7) =  l       .asInstanceOf[Byte]
    offs += 8
  }

  def shortBytes(bytes: Array[Byte]) = {
    short(bytes.length)
    ensureCapacity(bytes.length)
    System.arraycopy(bytes, 0, buffer, offs, bytes.length)
    offs += bytes.length
  }

  // ---- derived buffer ops

  def string(s: String) = shortBytes (s.getBytes(utf8))

  def stringMap(kvs: (String,String)*) = {
    short(kvs.length)
    for (kv <- kvs) {
      string(kv._1)
      string(kv._2)
    }
  }

  // ---- header

  buffer(0) = 0x04 // version
  buffer(1) = flags.b
  buffer(2) = (stream >> 8).asInstanceOf[Byte]
  buffer(3) =  stream      .asInstanceOf[Byte]
  buffer(4) = opCode.asInstanceOf[Byte]

  offs = 9 // leave four bytes for setting the frame's length when we are done

  // ---- finalize

  def build() = {
    // set the 'length' field now that we know the actual length
    val length = offs - 9
    buffer(5) = (length >> 24).asInstanceOf[Byte]
    buffer(6) = (length >> 16).asInstanceOf[Byte]
    buffer(7) = (length >> 8).asInstanceOf[Byte]
    buffer(8) = length.asInstanceOf[Byte]

    val result = ByteString.fromArrayUnsafe(buffer, 0, offs)
    buffer = null // to be on the safe side
    result
  }
}

private[protocol_v4] object CassandraFrameBuilder {
  val utf8 = Charset.forName("utf-8")
}
