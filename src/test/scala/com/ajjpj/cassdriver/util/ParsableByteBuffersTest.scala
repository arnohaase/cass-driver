package com.ajjpj.cassdriver.util

import java.nio.ByteBuffer

import com.ajjpj.cassdriver.AbstractCassDriverTest


class ParsableByteBuffersTest extends AbstractCassDriverTest {
  private def parsableBuffer(bytes: Int*) = {
    ParsableByteBuffers(Seq(buffer(bytes:_*)))
  }
  private def buffer(bytes: Int*) = {
    val buffer = ByteBuffer.allocate(bytes.size)
    for (b <- bytes) buffer.put(b.asInstanceOf[Byte])
    buffer.flip()
    buffer
  }


  "An unsigned short" should "be parsed" in {
    parsableBuffer(0,1).readUnsignedShort() should be (1)
    parsableBuffer(0,2).readUnsignedShort() should be (2)
    parsableBuffer(1,0).readUnsignedShort() should be (256)
    parsableBuffer(2,0).readUnsignedShort() should be (512)
    parsableBuffer(255,255).readUnsignedShort() should be (65535)
  }

  it should "be parsed if it spans two ByteBuffers" in {
    ParsableByteBuffers(Seq(buffer(1), buffer(2))).readUnsignedShort() should be (258)
  }

  "A string" should "be parsed" in {
    parsableBuffer(0, 1, 'A').readString() should be ("A")
    parsableBuffer(0, 3, 'A', 'b', 'C').readString() should be ("AbC")
    parsableBuffer(0, 2, 'A', 'b', 'C').readString() should be ("Ab")
  }

  it should "be parsed respecting its length" in {
    val buf = parsableBuffer(0, 1, 'A', 0, 1, 'B')
    buf.readString() should be ("A")
    buf.readString() should be ("B")
  }

  it should "be parsed if it spans two ByteBuffers" in {
    ParsableByteBuffers(Seq(buffer(0, 4), buffer('a', 'b', 'c', 'd'))).readString() should be ("abcd")
    ParsableByteBuffers(Seq(buffer(0, 4, 'a'), buffer('b', 'c', 'd'))).readString() should be ("abcd")
    ParsableByteBuffers(Seq(buffer(0, 4, 'a', 'b'), buffer('c', 'd'))).readString() should be ("abcd")
    ParsableByteBuffers(Seq(buffer(0, 4, 'a', 'b', 'c'), buffer('d'))).readString() should be ("abcd")
  }

  it should "be parsed if it spans three ByteBuffers" in {
    ParsableByteBuffers(Seq(buffer(0, 4), buffer('a', 'b'), buffer('c', 'd'))).readString() should be ("abcd")
    ParsableByteBuffers(Seq(buffer(0, 4, 'a'), buffer('b'), buffer('c', 'd'))).readString() should be ("abcd")
    ParsableByteBuffers(Seq(buffer(0, 4, 'a', 'b'), buffer('c'), buffer('d'))).readString() should be ("abcd")

    println ("äöü".getBytes("utf-8").map(_ & 255).mkString(","))
  }

  it should "be parsed as UTF-8" in {
    parsableBuffer(0, 6, 195, 164, 195, 182, 195, 188).readString() should be ("äöü")
  }
}
