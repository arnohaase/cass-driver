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

  "A Byte" should "be parsed" in {
    parsableBuffer(0).readByte() should be (0)
    parsableBuffer(1).readByte() should be (1)
    parsableBuffer(128).readByte() should be (128)
    parsableBuffer(255).readByte() should be (255)

    val buf = parsableBuffer(10, 99)
    buf.readByte() should be (10)
    buf.readByte() should be (99)
  }

  "An unsigned Short" should "be parsed" in {
    parsableBuffer(0,1).readUnsignedShort() should be (1)
    parsableBuffer(0,2).readUnsignedShort() should be (2)
    parsableBuffer(1,0).readUnsignedShort() should be (256)
    parsableBuffer(2,0).readUnsignedShort() should be (512)
    parsableBuffer(255,255).readUnsignedShort() should be (65535)
  }

  it should "be parsed if it spans two ByteBuffers" in {
    ParsableByteBuffers(Seq(buffer(1), buffer(2))).readUnsignedShort() should be (258)
  }

  "An Int" should "be parsed" in {
    parsableBuffer(0, 0, 0, 1).readInt() should be (1)
    parsableBuffer(1, 2, 3, 4).readInt() should be (0x01020304)
  }

  it should "be parsed as a signed int" in {
    parsableBuffer(128, 0, 0, 0).readInt() should be (Integer.MIN_VALUE)
    parsableBuffer(255,255,255,255).readInt() should be (-1)
  }

  it should "be parsed if it spans two ByteBuffers" in {
    ParsableByteBuffers(Seq(buffer(), buffer(1, 2, 3, 4))).readInt() should be (0x01020304)
    ParsableByteBuffers(Seq(buffer(1), buffer(2, 3, 4))).readInt() should be (0x01020304)
    ParsableByteBuffers(Seq(buffer(1, 2), buffer(3, 4))).readInt() should be (0x01020304)
    ParsableByteBuffers(Seq(buffer(1, 2, 3), buffer(4))).readInt() should be (0x01020304)

    ParsableByteBuffers(Seq(buffer(255, 255), buffer(255, 255))).readInt() should be (-1)
  }

  it should "be parsed if it spans three ByteBuffers" in {
    ParsableByteBuffers(Seq(buffer(1), buffer(2), buffer(3, 4))).readInt() should be (0x01020304)
    ParsableByteBuffers(Seq(buffer(1), buffer(2, 3), buffer(4))).readInt() should be (0x01020304)
  }

  "A Long" should "be parsed" in {
    parsableBuffer(0, 0, 0, 0, 0, 0, 0, 1).readLong() should be (1L)
    parsableBuffer(1, 2, 3, 4, 5, 6, 7, 8).readLong() should be (0x0102030405060708L)
  }

  it should "be parsed as a signed long" in {
    parsableBuffer(128, 0, 0, 0, 0, 0, 0, 0).readLong() should be (Long.MinValue)
    parsableBuffer(255,255,255,255,255,255,255,255).readLong() should be (-1L)
  }

  it should "be parsed if it spans two ByteBuffers" in {
    ParsableByteBuffers(Seq(buffer(1, 2, 3), buffer(4, 5, 6, 7, 8))).readLong() should be (0x0102030405060708L)
    ParsableByteBuffers(Seq(buffer(1, 2, 3, 4, 5, 6), buffer(7, 8))).readLong() should be (0x0102030405060708L)

    ParsableByteBuffers(Seq(buffer(255,255,255), buffer(255,255,255,255,255))).readLong() should be (-1L)
  }

  it should "be parsed if it spans three ByteBuffers" in {
    ParsableByteBuffers(Seq(buffer(1, 2, 3), buffer(4), buffer(5, 6, 7, 8))).readLong() should be (0x0102030405060708L)
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
  }

  it should "be parsed as UTF-8" in {
    parsableBuffer(0, 6, 195, 164, 195, 182, 195, 188).readString() should be ("äöü")
  }

  "A LongString" should "be parsed" in {
    parsableBuffer(0, 0, 0, 1, 'A').readLongString() should be ("A")
    parsableBuffer(0, 0, 0, 3, 'A', 'b', 'C').readLongString() should be ("AbC")
    parsableBuffer(0, 0, 0, 2, 'A', 'b', 'C').readLongString() should be ("Ab")
  }

  it should "be parsed respecting its length" in {
    val buf = parsableBuffer(0, 0, 0, 1, 'A', 0, 0, 0, 1, 'B')
    buf.readLongString() should be ("A")
    buf.readLongString() should be ("B")
  }

  it should "be parsed if it spans two ByteBuffers" in {
    ParsableByteBuffers(Seq(buffer(0, 0, 0, 4), buffer('a', 'b', 'c', 'd'))).readLongString() should be ("abcd")
    ParsableByteBuffers(Seq(buffer(0, 0, 0, 4, 'a'), buffer('b', 'c', 'd'))).readLongString() should be ("abcd")
    ParsableByteBuffers(Seq(buffer(0, 0, 0, 4, 'a', 'b'), buffer('c', 'd'))).readLongString() should be ("abcd")
    ParsableByteBuffers(Seq(buffer(0, 0, 0, 4, 'a', 'b', 'c'), buffer('d'))).readLongString() should be ("abcd")
  }

  it should "be parsed if it spans three ByteBuffers" in {
    ParsableByteBuffers(Seq(buffer(0, 0, 0, 4), buffer('a', 'b'), buffer('c', 'd'))).readLongString() should be ("abcd")
    ParsableByteBuffers(Seq(buffer(0, 0, 0, 4, 'a'), buffer('b'), buffer('c', 'd'))).readLongString() should be ("abcd")
    ParsableByteBuffers(Seq(buffer(0, 0, 0, 4, 'a', 'b'), buffer('c'), buffer('d'))).readLongString() should be ("abcd")
  }

  it should "be parsed as UTF-8" in {
    parsableBuffer(0, 0, 0, 6, 195, 164, 195, 182, 195, 188).readLongString() should be ("äöü")
  }

  it should "be parsed if it is longer than 32767 bytes" in {
    val byteArray = Array.ofDim[Byte](0x20000)
    for (i <- 0 until 0x20000) byteArray(i) = 'A'
    val str = ParsableByteBuffers(Seq(buffer(0, 2, 0, 0), ByteBuffer.wrap(byteArray))).readLongString()
    str.length should be (0x20000)
    str.replace("A", "") shouldBe empty
  }

  "CassBytes" should "be parsed" in {
    parsableBuffer(0, 0, 0, 0).readBytes().b shouldBe empty

    val b = parsableBuffer(0, 0, 0, 2, 99, 15).readBytes().b
    b.length should be (2)
    b(0) should be (99)
    b(1) should be (15)
  }

  it should "be parsed if it spans two ByteBuffers" in {
    val b = ParsableByteBuffers(Seq(buffer(0, 0, 0, 2, 99), buffer(15))).readBytes().b
    b.length should be (2)
    b(0) should be (99)
    b(1) should be (15)
  }

  it should "be parsed if it spans three ByteBuffers" in {
    val b = ParsableByteBuffers(Seq(buffer(0, 0, 0, 3, 99), buffer(15), buffer(123))).readBytes().b
    b.length should be (3)
    b(0) should be (99)
    b(1) should be (15)
    b(2) should be (123)
  }

  it should "be parsed if it is a big array" in {
    val byteArray = Array.fill[Byte](0x20000)(25)
    val b = ParsableByteBuffers(Seq(buffer(0, 2, 0, 0), ByteBuffer.wrap(byteArray))).readBytes().b
    b.length should be (0x20000)
    b.forall(_ == 25) should be (true)
  }

  it should "be parsed as NULL for length < 0" in {
    parsableBuffer(255, 255, 255, 255).readBytes() shouldBe null.asInstanceOf[CassBytes]
    parsableBuffer(128, 0, 0, 0).readBytes() shouldBe null.asInstanceOf[CassBytes]
    parsableBuffer(200, 10, 99, 15).readBytes() shouldBe null.asInstanceOf[CassBytes]
  }

  "mark()" should "create a snapshot that can be restored by reset()" in {
    val buf = parsableBuffer(99, 0, 0, 0, 1, 0, 0, 0, 2)
    buf.readByte() should be (99)

    buf.mark()

    buf.readInt() should be (1)
    buf.readInt() should be (2)

    buf.reset()

    buf.readInt() should be (1)
    buf.readInt() should be (2)

    buf.reset()

    buf.readInt() should be (1)
    buf.readInt() should be (2)
  }

  it should "work even if processing progressed to a different ByteBuffer" in {
    val buf = ParsableByteBuffers(Seq(buffer(99, 0), buffer(0, 0), buffer(1, 0), buffer(0, 0, 2, 123)))
    buf.readByte() should be (99)

    buf.mark()

    buf.readInt() should be (1)
    buf.readInt() should be (2)

    buf.reset()

    buf.readInt() should be (1)
    buf.readInt() should be (2)

    buf.reset()

    buf.readInt() should be (1)
    buf.readInt() should be (2)
  }

  "remaining" should "return the total number of unprocessed bytes across all ByteBuffers" in {
    val buf = ParsableByteBuffers(Seq(buffer(99, 0), buffer(0, 0), buffer(1, 0), buffer(0, 0, 2, 123)))
    buf.remaining should be (10)

    buf.readByte()
    buf.remaining should be (9)

    buf.readInt()
    buf.remaining should be (5)

    buf.readInt()
    buf.remaining should be (1)

    buf.readByte()
    buf.remaining should be (0)
  }
}
