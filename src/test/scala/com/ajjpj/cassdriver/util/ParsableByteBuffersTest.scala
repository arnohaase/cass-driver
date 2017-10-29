package com.ajjpj.cassdriver.util

import java.nio.ByteBuffer

import com.ajjpj.cassdriver.AbstractCassDriverTest


class ParsableByteBuffersTest extends AbstractCassDriverTest {
  private def parsableBuffer(bytes: Int*) = {
    ParsableByteBuffers(Seq(bufferFromBytes(bytes:_*)))
  }
  private def bufferFromBytes(bytes: Int*) = {
    val buffer = ByteBuffer.allocate(bytes.size)
    for (b <- bytes) buffer.put(b.asInstanceOf[Byte])
    buffer.flip()
    buffer
  }


  "A ParsableByteBuffers" should "parse an unsigned short" in {
    parsableBuffer(0,1).readUnsignedShort() should be (1)
    parsableBuffer(0,2).readUnsignedShort() should be (2)
    parsableBuffer(1,0).readUnsignedShort() should be (256)
    parsableBuffer(2,0).readUnsignedShort() should be (512)
    parsableBuffer(255,255).readUnsignedShort() should be (65535)
  }

  it should "parse a string" in {
    parsableBuffer(0, 1, 'A').readString() should be ("A")
    parsableBuffer(0, 3, 'A', 'b', 'C').readString() should be ("AbC")
    parsableBuffer(0, 2, 'A', 'b', 'C').readString() should be ("Ab")
  }

  it should "parse two consecutive strings" in {
    val buf = parsableBuffer(0, 1, 'A', 0, 1, 'B')
    buf.readString() should be ("A")
    buf.readString() should be ("B")
  }

  //TODO äöü --> UTF-8
}
