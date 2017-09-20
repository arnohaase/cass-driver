package com.ajjpj.cassdriver.util

import java.nio.ByteBuffer
import java.nio.charset.Charset


/**
  * This is a thin abstraction on top of ByteBuffer, providing support for all primitives required by the Cassandra
  *  spec and treating a sequence of ByteBuffers as a single parsable entity.
  */
trait ParsableByteSequence {
  def remaining: Int

  def mark(): Unit
  def reset(): Unit

  def byte(): Int
  def short(): Int
  def int(): Int
  def long(): Long

  def shortString(): String
  def longString(): String
}

object ParsableByteSequence {
  private val utf8 = Charset.forName("utf-8")

  def apply(buffers: Seq[ByteBuffer]): ParsableByteSequence = new ConcatParsableByteSequence(buffers)

  private class ConcatParsableByteSequence(private var buffers: Seq[ByteBuffer]) extends ParsableByteSequence {
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

    override def byte() = {
      headRemaining // trigger progression to next buffer if necessary
      head.get & 0xff
    }

    // *unsigned* short
    override def short() = if (nextUnfragmented(2))
      head.getShort & 0xffff
    else {
      var result = 0
      result += byte() << 8
      result += byte()
      result
    }


    override def int() = if (nextUnfragmented(4))
      head.getInt
    else {
      var result = 0
      result += byte() << 24
      result += byte() << 16
      result += byte() << 8
      result += byte()
      result
    }

    override def long() = if (nextUnfragmented(8))
      head.getLong
    else {
      var result = 0L
      result += byte() << 56
      result += byte() << 48
      result += byte() << 40
      result += byte() << 32
      result += byte() << 24
      result += byte() << 16
      result += byte() << 8
      result += byte()
      result
    }

    override def shortString() = stringRaw(short())
    override def longString() = stringRaw(int())

    private def stringRaw(numBytes: Int): String = {
      if (nextUnfragmented(numBytes)) {
        val cb = utf8.decode(head)
        new String (cb.array(), 0, cb.position)
      }
      else {
        ??? //TODO copy into a contiguous byte array, then decode
      }
    }
  }
}