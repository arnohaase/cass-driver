package com.ajjpj.cassdriver.connection.api


class CassConsistency(val v: Int) extends AnyVal {

}

object CassConsistency {
  final val ANY          = new CassConsistency(0x00)
  final val ONE          = new CassConsistency(0x01)
  final val TWO          = new CassConsistency(0x02)
  final val THREE        = new CassConsistency(0x03)
  final val QUORUM       = new CassConsistency(0x04)
  final val ALL          = new CassConsistency(0x05)
  final val LOCAL_QUORUM = new CassConsistency(0x06)
  final val EACH_QUORUM  = new CassConsistency(0x07)
  final val SERIAL       = new CassConsistency(0x08)
  final val LOCAL_SERIAL = new CassConsistency(0x09)
  final val LOCAL_ONE    = new CassConsistency(0x0a)
}
