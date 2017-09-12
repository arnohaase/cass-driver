package com.ajjpj.cassdriver.connection

import java.net.InetAddress

import akka.actor.Props
import com.ajjpj.cassdriver.AbstractCassDriverTest
import com.ajjpj.cassdriver.connection.protocol_v4.{MessageFlags, ProtocolV4}


class NaiveCassandraConnectionTest extends AbstractCassDriverTest {
  "A NaiveCassandraConnection" should "connect to Cassandra" in {
    val config = CassandraConnectionConfig(CassandraClusterConfig(), InetAddress.getLoopbackAddress, 9042)
    val conn = system.actorOf(Props(new NaiveCassandraConnection(config)))

    conn ! ProtocolV4.createStartupMessage(MessageFlags(false, false, false))

    Thread.sleep(1000)
  }

  "A ChannelCassandraConnection" should "connect to Cassandra" in {
    val config = CassandraConnectionConfig(CassandraClusterConfig(), InetAddress.getLoopbackAddress, 9042)
    val conn = system.actorOf(Props(new AsyncSocketChannelCassandraConnection(config, self)))

    expectMsg(AsyncSocketChannelCassandraConnection.Initialized)

    conn ! AsyncSocketChannelCassandraConnection.RawRequest (ProtocolV4.createStartupMessage(MessageFlags(false, false, false)))

    Thread.sleep(1000)
  }
}
