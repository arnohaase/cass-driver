package com.ajjpj.cassdriver.util

import java.nio.charset.Charset
import java.nio.{ByteBuffer, CharBuffer}

import com.ajjpj.cassdriver.connection.api.{CassConsistency, CassTimestamp}

import scala.collection.mutable.ArrayBuffer

/**
  * This is a builder for (one or more) byte buffers containing a variable length message.
  */
class ByteBuffersBuilder {
  private val utf8 = Charset.forName("utf-8")
  private val utf8Encoder = utf8.newEncoder()

  //TODO tuning does it make sense to use DirectByteBuffers here? Even pool them? Or does compression necessitate copying to a different set of buffers anyway?
  //TODO does Cassandra's protocol support sending uncompressed frames, and receiving compressed frames?!

  final val CHUNK_SIZE = 4096 //TODO tuning what is a good chunk size? --> compression does not make that easier... Does the chunk size even have measurable impact?

  private def newBuffer(size: Int) = ByteBuffer.allocate(size)

  private var curBuffer = newBuffer(CHUNK_SIZE)
  private val buffers = ArrayBuffer(curBuffer)

  /**
    * @return the 'current' buffer, with its 'current' position. This breaks encapsulation and is *very* low level, be really sure you know what you are doing if you use this
    */
  def snapshot = curBuffer

  def size = buffers.foldLeft(0)((r,next) => r + next.position)

  def build(): Array[ByteBuffer] = {
    buffers.foreach(_.flip())
    val result = buffers.toArray
    buffers.clear()
    result
  }

  //--------------------- write operations

  private def ensureCapacity(numBytes: Int) {
    if(curBuffer.remaining < numBytes) {
      curBuffer = ByteBuffer.allocate(Math.max(CHUNK_SIZE, numBytes))
      buffers += curBuffer
    }
  }

  def writeByte(b: Int) = {
    ensureCapacity(1)
    curBuffer.put(b.asInstanceOf[Byte])
    this
  }
  def writeShort(s: Int) = {
    ensureCapacity(2)
    curBuffer.putShort(s.asInstanceOf[Short])
    this
  }
  def writeInt(i: Int) = {
    ensureCapacity(4)
    curBuffer.putInt(i)
    this
  }
  def writeLong(l: Long) = {
    ensureCapacity(8)
    curBuffer.putLong(l)
    this
  }

  def writeBytes(bytes: Array[Byte]) = {
    writeInt(bytes.length)
    ensureCapacity(bytes.length)
    curBuffer.put(bytes)
    this
  }

  def writeShortString(s: String) = {
    utf8Encoder.reset()
    val chars = CharBuffer.wrap(s)

    // leave space and remember position to later write the encoded string size in bytes
    ensureCapacity(2)
    val lengthBuffer = curBuffer
    val lengthPos = lengthBuffer.position
    lengthBuffer.position(lengthPos + 2)

    var stringSizeInBytes = 0
    while (chars.remaining > 0) {
      val startPos = curBuffer.position
      utf8Encoder.encode(chars, curBuffer, true)
      stringSizeInBytes += (curBuffer.position - startPos)

      if (chars.remaining > 0) {
        ensureCapacity(utf8Encoder.maxBytesPerChar.toInt)
      }
    }

    require(stringSizeInBytes <= 0xffff)
    lengthBuffer.putShort(lengthPos, stringSizeInBytes.asInstanceOf[Short])
  }

  def writeLongString(s: String) = {
    utf8Encoder.reset()
    val chars = CharBuffer.wrap(s)

    // leave space and remember position to later write the encoded string size in bytes
    ensureCapacity(4)
    val lengthBuffer = curBuffer
    val lengthPos = lengthBuffer.position
    lengthBuffer.position(lengthPos + 4)

    var stringSizeInBytes = 0
    while (chars.remaining > 0) {
      val startPos = curBuffer.position
      utf8Encoder.encode(chars, curBuffer, true)
      stringSizeInBytes += (curBuffer.position - startPos)

      if (chars.remaining > 0) {
        ensureCapacity(utf8Encoder.maxBytesPerChar.toInt)
      }
    }

    lengthBuffer.putInt(lengthPos, stringSizeInBytes)
  }

  def writeStringMap(kvs: (String,String)*) = {
    writeShort(kvs.length)
    for(kv <- kvs) {
      writeShortString(kv._1)
      writeShortString(kv._2)
    }
  }



  //TODO the below methods are not strictly independent of the protocol version --> move them to the protocol implementation?
  def writeConsistency(consistency: CassConsistency) = writeShort(consistency.v)
  def writeTimestamp(timestamp: CassTimestamp) = writeLong(timestamp.l)
}
