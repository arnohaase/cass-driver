package com.ajjpj.cassdriver.connection.protocol_v4


class MessageFlags(val b: Byte) extends AnyVal {
  def hasCompression =   (b & 0x01) != 0
  def hasTracing =       (b & 0x02) != 0
  def hasCustomPayload = (b & 0x04) != 0
  def hasWarnings =      (b & 0x08) != 0

  override def toString = {
    def flag(name: String, value: Boolean) = s"$name: $value"
    s"MessageFlags: (${flag("hasCompression", hasCompression)}, ${flag("hasTracing", hasTracing)}, ${flag("hasCustomPayload", hasCustomPayload)}, ${flag("hasWarnings", hasWarnings)})"
  }
}

object MessageFlags {
  /**
    * requests can not have warnings, so there is no need to pass the 'warning' flag to this factory method
    */
  def apply (hasCompression: Boolean, hasTracing: Boolean, hasCustomPayload: Boolean) = {
    var b: Int = 0
    if (hasCompression) b += 0x01
    if (hasTracing) b += 0x02
    if (hasCustomPayload) b += 0x03
    new MessageFlags(b.asInstanceOf[Byte])
  }
}