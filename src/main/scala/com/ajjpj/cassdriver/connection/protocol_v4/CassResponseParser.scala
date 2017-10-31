package com.ajjpj.cassdriver.connection.protocol_v4

import com.ajjpj.cassdriver.connection.api.FullyQualifiedTableName
import com.ajjpj.cassdriver.connection.messages.{CassResponse, CassRowsResult, ErrorResponse, ReadyResponse}
import com.ajjpj.cassdriver.connection.messages.CassRowsResult.{CassRow, RowsResultFlags}
import com.ajjpj.cassdriver.connection.metadata._
import com.ajjpj.cassdriver.util.{CassBytes, ParsableByteBuffers}



class CassResponseParser(flags: MessageFlags, stream: Int, bodyLength: Int, frame: ParsableByteBuffers) {
  def parseError(): CassResponse = {
    val errorCode = frame.readInt()
    val errorMessage = frame.readString()
    ErrorResponse(flags, stream, bodyLength, errorCode, errorMessage)
  }

  def parseReady() = {
    require(bodyLength == 0)
    ReadyResponse(flags, stream, bodyLength)
  }

  def parseResult () = {
    val kind = frame.readInt()
    kind match {
      case 0x01 => ??? // Void
      case 0x02 => parseRowsResult()
      case 0x03 => ??? // SetKeyspace
      case 0x04 => ??? // Prepared
      case 0x05 => ??? // Schema Change
    }
  }

  private def parseRowsResult() = {
    val rowsFlags = new RowsResultFlags(frame.readInt())
    val colCount = frame.readInt()
    val pagingState = if (rowsFlags.hasMorePages) frame.readBytes() else null

    val globalTablesSpec = if(rowsFlags.hasGlobalTablesSpec) Some(FullyQualifiedTableName(frame.readString(), frame.readString())) else None

    val colSpecs = if (rowsFlags.hasNoMetadata)
      ???
    else (1 to colCount).map(_ => {
      val (keyspace, tableName) = globalTablesSpec match {
        case Some(FullyQualifiedTableName(ks, tn)) => (ks, tn)
        case None => (frame.readString(), frame.readString())
      }
      val colName = frame.readString()
      val tpe = CassType.read(frame)

      CassColumnMetadata(keyspace, tableName, colName, tpe)
    })

    require (colSpecs.size == colCount) // for no_metadata

    val rowsCount = frame.readInt()

    //TODO tuning copy rows to a ByteString, parse lazily?
    val rowsBuilder = Vector.newBuilder[CassRow]
    (1 to rowsCount).foreach {_ => {
      val rowBuilder = Vector.newBuilder[Any]
      for (colSpec <- colSpecs) {
        rowBuilder += colSpec.tpe.deser(frame)
      }
      rowsBuilder += CassRow(rowBuilder.result())
    }}

    CassRowsResult(flags, stream, bodyLength, rowsFlags, rowsBuilder.result())
  }
}

