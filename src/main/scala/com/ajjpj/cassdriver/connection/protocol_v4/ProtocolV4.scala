package com.ajjpj.cassdriver.connection.protocol_v4

import akka.util.ByteString


/**
  * This is a low-level implementation of https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec
  */
object ProtocolV4 {
  val HEADER_LENGTH = 9

  private val OPCODE_ERROR = 0x00
  private val OPCODE_STARTUP = 0x01
  private val OPCODE_READY = 0x02
  private val OPCODE_AUTHENTICATE = 0x03
  private val OPCODE_OPTIONS = 0x05
  private val OPCODE_SUPPORTED = 0x06
  private val OPCODE_QUERY = 0x07
  private val OPCODE_RESULT = 0x08
  private val OPCODE_PREPARE = 0x09
  private val OPCODE_EXECUTE = 0x0A
  private val OPCODE_REGISTER = 0x0B
  private val OPCODE_EVENT = 0x0C
  private val OPCODE_BATCH = 0x0D
  private val OPCODE_AUTH_CHALLENGE = 0x0E
  private val OPCODE_AUTH_RESPONSE = 0x0F
  private val OPCODE_AUTH_SUCCESS = 0x10

  def createStartupMessage (flags: MessageFlags): ByteString = {
    val builder = new CassandraFrameBuilder(flags, 0, OPCODE_STARTUP)
    builder.stringMap("CQL_VERSION" -> "3.2.1") //TODO CQL version
    //TODO compression
    builder.build()
  }

  /**
    * @param raw The buffers with currently unprocessed response data. There may be more than one such buffer, and
    *            a single response can span an arbitrary number of such responses - the response stream is split into
    *            buffers by low-level network operations.
    * @param offs The offset of the first unprocessed byte in the first buffer
    *
    * @return The parsed response message, if the 'raw' parameter contains the entire message, or None if more data
    *         is required to complete parsing. Invalid responses trigger exceptions (rather than causing None to be
    *         returned) and are assumed to invalidate the entire connection
    */
  def parseResponse(raw: Seq[ByteString], offs: Int): Option[Response] = {
    val bs: ByteString = if (raw.isEmpty)
      ByteString.empty
    else raw
      .tail
      .foldLeft(raw.head)((r,it) => r ++ it)
      .drop(offs)

    if (bs.length < HEADER_LENGTH) {
      // the buffer does not even contain the entire header - it's no use trying to parse anything
      None
    }
    else {
      require ((bs(0) & 0xff) == 0x84)
      val flags = new MessageFlags(bs(1))
      val stream = readShort(bs, 2)
      val opcode = bs(4)
      val length = readInt(bs, 5)
      require (length >= 0)
      require (length <= Integer.MAX_VALUE - HEADER_LENGTH) //TODO this formally deviates from the spec, but arrays (and ByteString etc.) are indexed by int, so dealing with it would be painful

      if (bs.length < HEADER_LENGTH + length) {
        None
      }
      else opcode match {
        case OPCODE_READY => Some(parseReady (flags, stream, length))
        case _ => throw new IllegalArgumentException (s"unsupported response opcode $opcode")
      }
    }
  }

  private def parseReady(flags: MessageFlags, stream: Int, length: Int): Response = {
    require(length == 0)
    ReadyResponse(flags, stream)
  }

  // ---- primitives

  private def readShort(bs: ByteString, offs: Int): Int = {
    var result = 0
    result += (bs(offs  ) & 0xff) << 8
    result += (bs(offs+1) & 0xff)
    result
  }

  private def readInt(bs: ByteString, offs: Int): Int = {
    var result = 0
    result += (bs(offs  ) & 0xff) << 24
    result += (bs(offs+1) & 0xff) << 16
    result += (bs(offs+2) & 0xff) << 8
    result += (bs(offs+3) & 0xff)
    result
  }
}
