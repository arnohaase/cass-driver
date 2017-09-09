package com.ajjpj.cassdriver.connection

import java.net.InetAddress

import akka.actor.Props
import akka.util.ByteString
import com.ajjpj.cassdriver.AbstractCassDriverTest


class NaiveCassandraConnectionTest extends AbstractCassDriverTest {
  "A NaiveCassandraConnection" should "connect to Cassandra" in {
    val config = CassandraConnectionConfig(CassandraClusterConfig(), InetAddress.getLoopbackAddress, 9042)
    val conn = system.actorOf(Props(new NaiveCassandraConnection(config)))

    conn ! ByteString(
      0x04, 0x00, 0x00, 0x00, 0x01,
      0x00, 0x00, 0x00, 0x16,
      0x00, 0x01,
      0x00, 0x0b, 'C', 'Q', 'L', '_', 'V', 'E', 'R', 'S', 'I', 'O', 'N',
      0x00, 0x05, '3', '.', '2', '.', '1'
    )

    Thread.sleep(5000)
  }
}
