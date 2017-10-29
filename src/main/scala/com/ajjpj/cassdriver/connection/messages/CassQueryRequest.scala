package com.ajjpj.cassdriver.connection.messages

import akka.util.ByteString
import com.ajjpj.cassdriver.connection.api.{CassConsistency, CassTimestamp}


//TODO tuning is passing in readily serialized params the best way to go? It removes the burden of parsing the query, looking up schema metadata, coercing types etc. from the connection, but still...
case class CassQueryRequest (query: String, consistency: CassConsistency, skipMetadata: Boolean, hasParams: Boolean, hasNamedParams: Boolean, params: ByteString,
                             resultPageSize: Int, pagingState: Option[ByteString], serialConsistency: CassConsistency = CassConsistency.SERIAL, timestamp: Option[CassTimestamp] = None)

object CassQueryRequest {
//  def serParams(columnTypes: Seq[CassType], values: Seq[Any])(implicit cassValues: CassValues): ByteString = {
//    require(columnTypes.size == values.size)
//
//    if (values.isEmpty) // this is an optimization TODO and requires the 'values' flag to be unset if there are no parameters
//      ByteString.empty
//    else {
//      val out = new ByteStringBuilder
//      ProtocolV4.writeShort(out, values.size)
//      for (i <- values.indices) cassValues.serialize(out, values(i), columnTypes(i))
//      out.result ()
//    }
//  }

  class CassQueryRequestFlags (val b: Byte) extends AnyVal {
    def hasSerialConsistency = (b & 0x10) != 0
  }

  /**
    * see 4.1.4 in the spec
    */
  object CassQueryRequestFlags {
    def apply (hasValues: Boolean, skipMetadata: Boolean, hasPageSize: Boolean, withPagingState: Boolean,
               withSerialConsistency: Boolean, withDefaultTimestamp: Boolean, withNamedValues: Boolean): CassQueryRequestFlags = {
      var b = 0
      if (hasValues) b += 0x01
      if (skipMetadata) b += 0x02
      if (hasPageSize) b += 0x04
      if (withPagingState) b += 0x08
      if (withSerialConsistency) b += 0x10
      if (withDefaultTimestamp) b += 0x20
      if (withNamedValues) b += 0x40

      new CassQueryRequestFlags(b.asInstanceOf[Byte])
    }
  }}
