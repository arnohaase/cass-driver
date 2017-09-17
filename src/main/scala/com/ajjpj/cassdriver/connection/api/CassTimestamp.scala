package com.ajjpj.cassdriver.connection.api

import java.time.Instant


class CassTimestamp(val l: Long) extends AnyVal {

}

object CassTimestamp {
  def forInstant(i: Instant) = new CassTimestamp(i.toEpochMilli * 1000 + (i.getNano / 1000) % 1000)
  def forMillis(millis: Long) = new CassTimestamp(millis * 1000)
}
