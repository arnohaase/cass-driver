package com.ajjpj.cassdriver.util

import java.nio.ByteBuffer
import java.nio.charset.Charset


/**
  * This is a thin abstraction on top of ByteBuffer, providing support for all primitives required by the Cassandra
  *  spec and treating a sequence of ByteBuffers as a single parsable entity.
  */
trait ParsableByteBuffers {
  def remaining: Int

  def mark(): Unit
  def reset(): Unit

  def readByte (): Int
  def readUnsignedShort (): Int
  def readInt (): Int
  def readLong (): Long

  def readString (): String
  def readLongString (): String
  def stringRaw (numBytes: Int): String

  def readBytes (): CassBytes
}

object ParsableByteBuffers {
  private val utf8 = Charset.forName("utf-8")

  def apply(buffers: Seq[ByteBuffer]): ParsableByteBuffers = new ConcatParsableByteSequence(buffers)

  private class ConcatParsableByteSequence(private var buffers: Seq[ByteBuffer]) extends ParsableByteBuffers {
    private def head = buffers.head

    //noinspection ScalaUnnecessaryParentheses
    private def headRemaining: Int = {
      while (! head.hasRemaining) buffers = buffers.tail
      head.remaining
    }

    private def nextUnfragmented(numBytes: Int) = headRemaining >= numBytes

    override def remaining = buffers.foldLeft(0)((r,n) => r + n.remaining)

    override def mark() = head.mark()
    override def reset() = head.reset() // This implicit deals with the case when 'head' became consumed and was discarded since calling mark(), in which case we fail explicitly. That is sufficient for current use cases of mark() / reset()

    override def readByte () = {
      headRemaining // trigger progression to next buffer if necessary
      head.get & 0xff
    }

    // *unsigned* short
    override def readUnsignedShort () = if (nextUnfragmented(2))
      head.getShort & 0xffff
    else {
      var result = 0
      result += readByte() << 8
      result += readByte()
      result
    }


    override def readInt () = if (nextUnfragmented(4))
      head.getInt
    else {
      var result = 0
      result += readByte() << 24
      result += readByte() << 16
      result += readByte() << 8
      result += readByte()
      result
    }

    override def readLong () = if (nextUnfragmented(8))
      head.getLong
    else {
      var result = 0L
      result += readByte() << 56
      result += readByte() << 48
      result += readByte() << 40
      result += readByte() << 32
      result += readByte() << 24
      result += readByte() << 16
      result += readByte() << 8
      result += readByte()
      result
    }

    override def readString () = stringRaw(readUnsignedShort())
    override def readLongString () = stringRaw(readInt())

    override def stringRaw(numBytes: Int): String = {
      if (nextUnfragmented(numBytes)) {
        val cb = utf8.decode(head)
        new String (cb.array(), 0, cb.position)
      }
      else {
        ??? //TODO copy into a contiguous byte array, then decode
      }
    }

    override def readBytes (): CassBytes = {
      val length = readInt()
      if (length < 0) CassBytes.NULL
      else {
        val result = Array.ofDim[Byte](length)
        for (i <- 0 until length) result(i) = readByte().asInstanceOf[Byte] //TODO tuning bulk read - but beware of fragmentation
        new CassBytes (result)
      }
    }
  }
}