package com.ajjpj.cassdriver.connection.protocol_v4

import akka.util.{ByteString, ByteStringBuilder}


private[protocol_v4] case class CassandraFrame (flags: MessageFlags, stream: Int, opCode: Int, body: ByteString) {
  val asByteString = {
    val out = new ByteStringBuilder

    // header
    ProtocolV4.writeByte(out, ProtocolV4.VERSION_REQUEST)
    ProtocolV4.writeByte(out, flags.b)
    ProtocolV4.writeShort(out, stream)
    ProtocolV4.writeByte(out, opCode)
    ProtocolV4.writeInt(out, body.length)

    // body
    out ++= body

    out.result()
  }
}

private[protocol_v4] class CassandraFrameBuilder(flags: MessageFlags, stream: Int, opCode: Int) {
  private val bodyBuilder = new ByteStringBuilder

  def stringMap(kvs: (String,String)*) = {
    ProtocolV4.writeStringMap(bodyBuilder, kvs:_*)
    this
  }

  def build() = CassandraFrame(flags, stream, opCode, bodyBuilder.result())
  def buildByteString() = build().asByteString
}