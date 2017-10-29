package com.ajjpj.cassdriver.util

/**
  * represents a 'bytes' data structure, i.e. a (potentially empty) sequence of bytes with an additional representation
  *  for 'null' which is distinct from an empty array
  */
class CassBytes(val b: Array[Byte]) extends AnyVal {
  import CassBytes._

  def isNull = this.b eq NULL.b
}
object CassBytes {
  val NULL = new CassBytes(Array.emptyByteArray)
}
