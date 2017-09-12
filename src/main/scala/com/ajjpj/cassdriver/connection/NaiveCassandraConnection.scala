package com.ajjpj.cassdriver.connection

import java.net.Socket

import akka.actor.Actor
import akka.util.ByteString
import com.ajjpj.cassdriver.connection.protocol_v4.ProtocolV4


/**
  * A CassandraConnection represents a socket connection of this client with one Cassandra host in a cluster
  */
class NaiveCassandraConnection (config: CassandraConnectionConfig) extends Actor {
  val socket = new Socket(config.address, config.port)

  new Thread() {
    override def run(): Unit = {
      var bs = ByteString.empty

      while (true) {
        bs = bs.concat(ByteString(socket.getInputStream.read().asInstanceOf[Byte]))

        ProtocolV4.parseResponse(Seq(bs), 0) match {
          case Some(r) =>
            println (r)
            bs = bs.drop(r.numBytes)
            println ("--> " + bs)
          case None =>
        }
      }
    }
  }.start()

  override def receive = {
    case s: ByteString =>
      print (" ==>")
      s.foreach(b => {
        val i: Int = b
        print (" " + Integer.toHexString(i & 0xff))
      })
      println
      socket.getOutputStream.write(s.toArray)
  }

  override def postStop () = {
    super.postStop ()
    socket.close()
  }
}
