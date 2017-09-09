package com.ajjpj.cassdriver.connection

import java.net.Socket

import akka.actor.Actor
import akka.util.ByteString


/**
  * A CassandraConnection represents a socket connection of this client with one Cassandra host in a cluster
  */
class NaiveCassandraConnection (config: CassandraConnectionConfig) extends Actor {
  val socket = new Socket(config.address, config.port)

  new Thread() {
    override def run(): Unit = {
      while (true) {
        val byte = socket.getInputStream.read()
        println("<==  " + Integer.toHexString(byte) + "\t" + byte.asInstanceOf[Char])
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
