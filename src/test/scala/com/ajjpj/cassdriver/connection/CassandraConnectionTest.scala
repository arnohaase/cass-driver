package com.ajjpj.cassdriver.connection

import java.net.InetAddress

import akka.actor.Props
import akka.util.ByteString
import com.ajjpj.cassdriver.AbstractCassDriverTest
import com.ajjpj.cassdriver.connection.api.CassConsistency
import com.ajjpj.cassdriver.connection.messages.{CassQueryRequest, CassResponse, ReadyResponse}


class CassandraConnectionTest extends AbstractCassDriverTest {
  "A ChannelCassandraConnection" should "connect to Cassandra" in {
    val config = CassandraConnectionConfig(CassandraClusterConfig(), InetAddress.getLoopbackAddress, 9042)
    val conn = system.actorOf(Props(new AsyncSocketChannelCassandraConnection(config, self)))
    expectMsg(AsyncSocketChannelCassandraConnection.Initialized)

    conn ! AsyncSocketChannelCassandraConnection.StartupRequest ("3.2.1")
    val reply = expectMsgType[ReadyResponse]
    println (reply)

    conn ! CassQueryRequest("select email, guest from control2.accounts", CassConsistency.ONE, skipMetadata=false, hasParams=false, hasNamedParams=false, ByteString.empty, 100, None)
//    conn ! CassQueryRequest("select * from control2.locks", CassConsistency.ONE, skipMetadata=false, hasParams=false, hasNamedParams=false, ByteString.empty, 100, None)
    val reply2 = expectMsgType[CassResponse]
    println (reply2)
    
//    Thread.sleep(1000)
  }
}
