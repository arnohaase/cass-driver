package com.ajjpj.cassdriver.connection

import java.net.InetAddress

import akka.actor.Props
import akka.util.ByteString
import com.ajjpj.cassdriver.AbstractCassDriverTest
import com.ajjpj.cassdriver.connection.api.{CassConsistency, QueryRequest}
import com.ajjpj.cassdriver.connection.protocol_v4.{ReadyResponse, CassResponse}


class CassandraConnectionTest extends AbstractCassDriverTest {
  "A ChannelCassandraConnection" should "connect to Cassandra" in {
    val config = CassandraConnectionConfig(CassandraClusterConfig(), InetAddress.getLoopbackAddress, 9042)
    val conn = system.actorOf(Props(new AsyncSocketChannelCassandraConnection(config, self)))
    expectMsg(AsyncSocketChannelCassandraConnection.Initialized)

    conn ! AsyncSocketChannelCassandraConnection.StartupRequest ("3.2.1")
    val reply = expectMsgType[ReadyResponse]
    println (reply)

    conn ! QueryRequest("select * from control2.locks", CassConsistency.ONE, false, false, false, ByteString.empty, 100, None)
    val reply2 = expectMsgType[CassResponse]
    println (reply2)
    
    Thread.sleep(1000)
  }
}
