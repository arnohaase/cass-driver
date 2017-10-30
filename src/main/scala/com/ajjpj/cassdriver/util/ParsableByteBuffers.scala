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

  def apply(buffers: Seq[ByteBuffer]): ParsableByteBuffers = new ConcatParsableByteSequence(buffers.toList)

  private class ConcatParsableByteSequence(private var buffers: List[ByteBuffer]) extends ParsableByteBuffers {
    private def head = buffers.head

    //noinspection ScalaUnnecessaryParentheses
    private def headRemaining: Int = {
      while (! head.hasRemaining) buffers = buffers.tail
      head.remaining
    }

    private def nextUnfragmented(numBytes: Int) = headRemaining >= numBytes

    override def remaining = buffers.foldLeft(0)((r,n) => r + n.remaining)

    private var snapshot = Option.empty[List[ByteBuffer]]
    override def mark() = {
      snapshot = Some(buffers)
      buffers.foreach(_.mark())
    }
    override def reset() = {
      buffers = snapshot.get
      buffers.foreach(_.reset())
    }


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
      result += readByte().asInstanceOf[Long] << 56
      result += readByte().asInstanceOf[Long] << 48
      result += readByte().asInstanceOf[Long] << 40
      result += readByte().asInstanceOf[Long] << 32
      result += readByte().asInstanceOf[Long] << 24
      result += readByte().asInstanceOf[Long] << 16
      result += readByte().asInstanceOf[Long] << 8
      result += readByte()
      result
    }

    override def readString () = stringRaw(readUnsignedShort())
    override def readLongString () = stringRaw(readInt())

    override def stringRaw(numBytes: Int): String = {
      if (nextUnfragmented(numBytes)) {
        val prevLimit = head.limit
        head.limit(head.position + numBytes)

        val cb = utf8.decode(head)

        head.limit(prevLimit)

        new String (cb.array(), 0, cb.limit)
      }
      else {
        //TODO tuning incremental decoding - decode the string in chunks to avoid a copy operation (?)
        val tmp = Array.ofDim[Byte](numBytes)
        var arrOffs = 0
        var toBeRead = numBytes

        while (toBeRead > 0) {
          val numInHead = Math.min(headRemaining, toBeRead)
          head.get (tmp, arrOffs, numInHead)
          arrOffs += numInHead
          toBeRead -= numInHead
        }

        new String (tmp, utf8)
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