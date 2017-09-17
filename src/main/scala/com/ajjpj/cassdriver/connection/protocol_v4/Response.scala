package com.ajjpj.cassdriver.connection.protocol_v4


sealed trait CassResponse {
  /**
    * @return the total number of bytes in the messages, i.e. including header and body
    */
  def numBytes: Int

  def flags: MessageFlags
  def stream: Int

}

case class ErrorResponse(numBytes: Int, flags: MessageFlags, stream: Int, errorCode: Int, errorMessage: String) extends CassResponse {

  //TODO error classification
  //TODO error handling
  //TODO specific additional payload for some errors
  /**
    * @return the total number of bytes in the messages, i.e. including header and body
    */
}

case class ReadyResponse(flags: MessageFlags, stream: Int) extends CassResponse {
  override def numBytes = ProtocolV4.HEADER_LENGTH
}
