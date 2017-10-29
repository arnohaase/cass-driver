package com.ajjpj.cassdriver.connection.protocol_v4

import java.nio.charset.Charset
import java.nio.{ByteBuffer, ByteOrder}

import com.ajjpj.cassdriver.connection.api.CassConsistency
import com.ajjpj.cassdriver.connection.messages.{CassQueryRequest, CassResponse}
import com.ajjpj.cassdriver.connection.messages.CassQueryRequest.CassQueryRequestFlags
import com.ajjpj.cassdriver.util.{ByteBuffersBuilder, ParsableByteBuffers}


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


  def createStartupMessage (stream: Int, cqlVersion: String): Array[ByteBuffer] = {
    //TODO compression
    //TODO tracing

    buildFrame(MessageFlags(false, false, false), stream, OPCODE_STARTUP)(bw => {
      bw.writeStringMap("CQL_VERSION" -> cqlVersion) //TODO CQL version enum
    })
  }

  private def buildFrame(msgFlags: MessageFlags, stream: Int, opCode: Int)(bodyWriter: ByteBuffersBuilder => Unit): Array[ByteBuffer] = {
    val result = new ByteBuffersBuilder

    // header
    result.writeByte(ProtocolV4.VERSION_REQUEST)
    result.writeByte(msgFlags.b)
    result.writeShort(stream)
    result.writeByte(opCode)

    result.writeInt(0) // placeholder for the actual body length, which we know only know when we are finished
    val bodySizeBuffer = result.snapshot
    val bodySizePos = bodySizeBuffer.position - 4

    // body
    bodyWriter(result)

    bodySizeBuffer.putInt(bodySizePos, result.size - HEADER_LENGTH)

    result.build()
  }

  //TODO make this return 'CassFrame' rather than 'ByteString' --> compression
  def createQueryMessage (stream: Int, queryRequest: CassQueryRequest): Array[ByteBuffer] = {
    buildFrame(MessageFlags(false, false, false), stream, OPCODE_QUERY)(bw => {
      val queryFlags = CassQueryRequestFlags (
        hasValues = queryRequest.hasParams,
        skipMetadata = queryRequest.skipMetadata,
        hasPageSize = true,
        withPagingState = queryRequest.pagingState.isDefined,
        withSerialConsistency = queryRequest.serialConsistency != CassConsistency.SERIAL,
        withDefaultTimestamp = queryRequest.timestamp.isDefined,
        withNamedValues = queryRequest.hasNamedParams
      )

      bw.writeLongString(queryRequest.query)
      bw.writeConsistency(queryRequest.consistency)
      bw.writeByte(queryFlags.b)
      if (queryRequest.hasParams) bw.writeBytes(queryRequest.params.toArray) //TODO tuning this copies twice
      bw.writeInt(queryRequest.resultPageSize)
      queryRequest.pagingState.foreach (x => bw.writeBytes(x.toArray)) //TODO tuning this copies twice
      if (queryFlags.hasSerialConsistency) bw.writeConsistency(queryRequest.serialConsistency)
      queryRequest.timestamp.foreach (bw.writeTimestamp)
    })
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
    val frame = ParsableByteBuffers(raw)

    if (frame.remaining < HEADER_LENGTH) {
      // the buffer does not even contain the entire header - it's no use trying to parse anything
      None
    }
    else {
      frame.mark()
      require (frame.readByte == VERSION_RESPONSE)
      val flags = new MessageFlags (frame.readByte().asInstanceOf[Byte])
      val stream = frame.readUnsignedShort()
      val opcode = frame.readByte()
      val bodyLength = frame.readInt()
      require (bodyLength >= 0)
      require (bodyLength <= Integer.MAX_VALUE - HEADER_LENGTH) //TODO this formally deviates from the spec, but arrays (and ByteString etc.) are indexed by int, so being 100% compliant would be painful

      if (frame.remaining < bodyLength) {
        frame.reset()
        None
      }
      else {
        val parser = new CassResponseParser(flags, stream, bodyLength, frame)
        opcode match {
          case OPCODE_ERROR => Some(parser.parseError())
          case OPCODE_READY => Some(parser.parseReady())
          case OPCODE_RESULT => Some(parser.parseResult())
          case _ => throw new IllegalArgumentException (s"unsupported response opcode $opcode") //TODO error handling
        }
      }
    }
  }

//  private def dump(bs: ByteString): Unit = {
//    println (bs.map(x => String.format("%02x", int2Integer(x & 0xff))).mkString(" "))
//    println (bs.map(x => (x & 0xff).toChar).mkString)
//  }
}
