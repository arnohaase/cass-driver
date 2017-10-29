package com.ajjpj.cassdriver.connection.messages

import com.ajjpj.cassdriver.connection.messages.CassRowsResult.{CassRow, RowsResultFlags}
import com.ajjpj.cassdriver.connection.protocol_v4.{MessageFlags, ProtocolV4}

sealed trait CassResponse {
  def bodyLength: Int
  final def numBytes = ProtocolV4.HEADER_LENGTH + bodyLength

  def flags: MessageFlags
  def stream: Int
}

case class ErrorResponse(flags: MessageFlags, stream: Int, bodyLength: Int, errorCode: Int, errorMessage: String) extends CassResponse {

  //TODO error classification
  //TODO error handling
  //TODO specific additional payload for some errors
}

case class ReadyResponse(flags: MessageFlags, stream: Int, bodyLength: Int) extends CassResponse


case class CassRowsResult(flags: MessageFlags, stream: Int, bodyLength: Int, rowsFlags: RowsResultFlags, rows: Vector[CassRow]) extends CassResponse
object CassRowsResult {
  class RowsResultFlags(val i: Int) extends AnyVal {
    def hasGlobalTablesSpec = (i & 0x01) != 0
    def hasMorePages        = (i & 0x02) != 0
    def hasNoMetadata       = (i & 0x04) != 0

    override def toString = {
      def flag(name: String, value: Boolean) = s"$name: $value"
      s"MessageFlags: (${flag("hasGlobalTablesSpec", hasGlobalTablesSpec)}, ${flag("hasMorePages", hasMorePages)}, ${flag("hasNoMetadata", hasNoMetadata)})"
    }
  }

  case class CassRow(values: Vector[Any])
}