package com.ajjpj.cassdriver.connection.protocol_v4


trait Response {
  /**
    * @return the total number of bytes in the messages, i.e. including header and body
    */
  def numBytes: Int

  def flags: MessageFlags
  def stream: Int

}

case class ReadyResponse(flags: MessageFlags, stream: Int) extends Response {
  override def numBytes = ProtocolV4.HEADER_LENGTH
}
