package com.ajjpj.cassdriver.connection.api

import akka.util.ByteStringBuilder


class CassValues {
  def serialize (out: ByteStringBuilder, o: Any, tpe: CassType): Unit = {
    if (o == null)
      tpe.serNull (out)
    else {
      tpe match {
        case CassTypeInt => CassTypeInt.serNotNull (out, coerce(o))
        case CassTypeBoolean => CassTypeBoolean.serNotNull(out, coerce(o))
        case CassTypeString => CassTypeString.serNotNull(out, coerce(o))
      }
    }
  }

  def coerce[T] (o: Any): T = {
    //TODO implement type coercion --> registry
    o.asInstanceOf[T]
  }
}
