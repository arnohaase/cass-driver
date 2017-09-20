package com.ajjpj.cassdriver.connection.protocol_v4

import java.nio.charset.Charset
import java.nio.{ByteBuffer, ByteOrder}

import akka.util.{ByteString, ByteStringBuilder}
import com.ajjpj.cassdriver.connection.api.{CassConsistency, CassTimestamp, QueryRequest, QueryRequestFlags}
import com.ajjpj.cassdriver.util.ParsableByteSequence


/**
  * This is a low-level implementation of https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec
  */
object ProtocolV4 {
  final val VERSION_REQUEST = 0x04.asInstanceOf[Byte]
  private final val VERSION_RESPONSE = 0x84

  final val HEADER_LENGTH = 9

  private final val OPCODE_ERROR = 0x00
  private final val OPCODE_STARTUP = 0x01
  private final val OPCODE_READY = 0x02
  private final val OPCODE_AUTHENTICATE = 0x03
  private final val OPCODE_OPTIONS = 0x05
  private final val OPCODE_SUPPORTED = 0x06
  private final val OPCODE_QUERY = 0x07
  private final val OPCODE_RESULT = 0x08
  private final val OPCODE_PREPARE = 0x09
  private final val OPCODE_EXECUTE = 0x0A
  private final val OPCODE_REGISTER = 0x0B
  private final val OPCODE_EVENT = 0x0C
  private final val OPCODE_BATCH = 0x0D
  private final val OPCODE_AUTH_CHALLENGE = 0x0E
  private final val OPCODE_AUTH_RESPONSE = 0x0F
  private final val OPCODE_AUTH_SUCCESS = 0x10

  private implicit final val byteOrder = ByteOrder.BIG_ENDIAN
  final val utf8 = Charset.forName("utf-8")


  def createStartupMessage (stream: Int, cqlVersion: String): ByteString = {
    //TODO compression
    //TODO tracing

    new CassandraFrameBuilder(MessageFlags(false, false, false), stream, OPCODE_STARTUP)
      .stringMap("CQL_VERSION" -> cqlVersion) //TODO CQL version enum
      .buildByteString()
  }

  //TODO make this return 'CassFrame' rather than 'ByteString' --> compression
  def createQueryMessage (stream: Int, queryRequest: QueryRequest) = {
    val body = {
      val out = new ByteStringBuilder

      val queryFlags = QueryRequestFlags (
        hasValues = queryRequest.hasParams,
        skipMetadata = queryRequest.skipMetadata,
        hasPageSize = true,
        withPagingState = queryRequest.pagingState.isDefined,
        withSerialConsistency = queryRequest.serialConsistency != CassConsistency.SERIAL,
        withDefaultTimestamp = queryRequest.timestamp.isDefined,
        withNamedValues = queryRequest.hasNamedParams
      )

      writeLongString(out, queryRequest.query)
      writeConsistency(out, queryRequest.consistency)
      writeByte(out, queryFlags.b)
      if (queryRequest.hasParams) writeBytes(out, queryRequest.params.toArray) //TODO tuning this copies twice
      writeInt(out, queryRequest.resultPageSize)
      queryRequest.pagingState.foreach (x => writeBytes(out, x.toArray)) //TODO tuning this copies twice
      if (queryFlags.hasSerialConsistency) writeConsistency(out, queryRequest.serialConsistency)
      queryRequest.timestamp.foreach (writeTimestamp(out, _))

      out.result()
    }

    CassandraFrame(MessageFlags(false, false, false), stream, OPCODE_QUERY, body)
      .asByteString
  }

  /**
    * @param raw The buffers with currently unprocessed response data. There may be more than one such buffer, and
    *            a single response can span an arbitrary number of such responses - the response stream is split into
    *            buffers by low-level network operations.
    *
    * @return The parsed response message, if the 'raw' parameter contains the entire message, or None if more data
    *         is required to complete parsing. Invalid responses trigger exceptions (rather than causing None to be
    *         returned) and are assumed to invalidate the entire connection
    */
  def parseResponse(raw: Seq[ByteBuffer]): Option[CassResponse] = {
    val frame = ParsableByteSequence(raw)

    if (frame.remaining < HEADER_LENGTH) {
      // the buffer does not even contain the entire header - it's no use trying to parse anything
      None
    }
    else {
      frame.mark()
      require (frame.byte == VERSION_RESPONSE)
      val flags = new MessageFlags (frame.byte().asInstanceOf[Byte])
      val stream = frame.short()
      val opcode = frame.byte()
      val bodyLength = frame.int()
      require (bodyLength >= 0)
      require (bodyLength <= Integer.MAX_VALUE - HEADER_LENGTH) //TODO this formally deviates from the spec, but arrays (and ByteString etc.) are indexed by int, so being 100% compliant would be painful

      if (frame.remaining < bodyLength) {
        frame.reset()
        None
      }
      else {
        opcode match {
          case OPCODE_ERROR => Some(parseError(flags, stream, bodyLength, frame))
          case OPCODE_READY => Some(parseReady (flags, stream, bodyLength))
          case OPCODE_RESULT => Some(parseResult (flags, stream, bodyLength, frame))
          case _ => throw new IllegalArgumentException (s"unsupported response opcode $opcode") //TODO error handling
        }
      }
    }
  }

  private def dump(bs: ByteString): Unit = {
    println (bs.map(x => String.format("%02x", int2Integer(x & 0xff))).mkString(" "))
    println (bs.map(x => (x & 0xff).toChar).mkString)
  }

  private def parseResult(flags: MessageFlags, stream: Int, bodyLength: Int, frame: ParsableByteSequence): CassResponse = ???

  private def parseError(flags: MessageFlags, stream: Int, bodyLength: Int, frame: ParsableByteSequence): CassResponse = {
    val errorCode = frame.int()
    val errorMessage = frame.shortString()
    ErrorResponse(HEADER_LENGTH + bodyLength, flags, stream, errorCode, errorMessage)
  }

  private def parseReady(flags: MessageFlags, stream: Int, length: Int): CassResponse = {
    require(length == 0)
    ReadyResponse(flags, stream)
  }

  // ---- primitives: write

  def writeByte(out: ByteStringBuilder, b: Int) = out.putByte(b.asInstanceOf[Byte])
  def writeShort(out: ByteStringBuilder, s: Int) = {
    writeByte(out, (s >> 8) & 0xff) //NB: shorts are *unsigned* per spec
    writeByte(out, (s >> 0) & 0xff)
  }
  def writeInt(out: ByteStringBuilder, i: Int) = out.putInt(i)
  def writeLong(out: ByteStringBuilder, l: Long) = out.putLong(l)

  def writeBytes(out: ByteStringBuilder, bytes: Array[Byte]): Unit = {
    writeInt(out, bytes.length)
    out.putBytes(bytes)
  }

  def writeShortString(out: ByteStringBuilder, s: String) = {
    val bytes = s.getBytes(utf8)
    writeShort(out, bytes.length)
    out.putBytes(bytes)
  }
  def writeLongString(out: ByteStringBuilder, s: String) = {
    val bytes = s.getBytes(utf8)
    writeInt(out, bytes.length)
    out.putBytes(bytes)
  }

  def writeStringMap(out: ByteStringBuilder, kvs: (String,String)*) = {
    writeShort(out, kvs.length)
    for(kv <- kvs) {
      writeShortString(out, kv._1)
      writeShortString(out, kv._2)
    }
  }

  def writeConsistency(out: ByteStringBuilder, consistency: CassConsistency) = writeShort(out, consistency.v)
  def writeTimestamp(out: ByteStringBuilder, timestamp: CassTimestamp) = writeLong(out, timestamp.l)
}
