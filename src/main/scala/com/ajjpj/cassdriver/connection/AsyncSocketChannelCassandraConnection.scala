package com.ajjpj.cassdriver.connection

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousSocketChannel, CompletionHandler}

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorRef}
import akka.util.ByteString
import com.ajjpj.cassdriver.connection.protocol_v4.ProtocolV4

import scala.annotation.tailrec
import scala.collection.mutable


object AsyncSocketChannelCassandraConnection {
  case object Initialized //TODO move to more generic 'API'?
  case class RawRequest (data: ByteString) //TODO move to more generic 'API'?

  private case object TrySend
  private case class DataSent(numBytes: Int)
  private case object TriggerRead
  private case class RawDataFromServer (data: ByteString)
}

class AsyncSocketChannelCassandraConnection (config: CassandraConnectionConfig, owner: ActorRef) extends Actor {
  import AsyncSocketChannelCassandraConnection._

  //TODO socket options
  val channel = AsynchronousSocketChannel.open()
  channel.connect(new InetSocketAddress(config.address, config.port), "", new CompletionHandler[Void, String] {
    override def failed (exc: Throwable, attachment: String) = self ! Failure(exc)
    override def completed (result: Void, attachment: String) = self ! Initialized
  })

  val readBuffer = ByteBuffer.allocate(16384)

  /**
    * raw snippets sent from the server, stored in chronological order. Presence of more than one item may be due
    *  to back logging, or because a single Response spans several chunks
    */
  val receivedQueue = mutable.ArrayBuffer.empty[ByteString]

  /**
    * send frames are queued so that send operations don't overlap: a frame is sent only after the previous frame
    *  was acknowledged by the network API. This is necessary because async channels do not guarantee to actually send
    *  all data passed to a send call.
    * It also automatically batches several frames in high-load scenarios, and it
    *  lays the foundation for explicit batching TODO tuning if explicit batching is even desirable
    */
  val sendQueue = mutable.ArrayBuffer.empty[ByteString]
  var isSending = false

  val initializing: Receive = {
    case Initialized =>
      //noinspection ForwardReference
      context.become(processing)
      owner ! Initialized
      self ! TriggerRead
    case msg@Failure(th) =>
      th.printStackTrace() //TODO logging
      context.stop(self)
  }

  val processing: Receive = {
    case TrySend =>
      trySend()
    case DataSent(numBytes) =>
      onDataSent(numBytes)

    case TriggerRead =>
      triggerRead()

    case RawRequest (data) if isSending =>
      sendQueue += data
    case RawRequest (data) =>
      print (" ==>")
      data.foreach(b => {
        val i: Int = b
        print (" " + Integer.toHexString(i & 0xff))
      })
      println

      sendQueue += data
      channel.write(data.toByteBuffer)

    case RawDataFromServer(bs) =>
      receivedQueue += bs
      tryParse()

    case msg@Failure(th) =>
      th.printStackTrace() //TODO logging
      context.stop(self)
  }

  override def receive = initializing

  //TODO tuning separate writing and reading / parsing to different actors?

  @tailrec
  private def tryParse(): Unit = {
    ProtocolV4.parseResponse (receivedQueue, 0) match {
      case Some(msg) =>
        println ("<==  " + msg) //TODO handling --> keep track of interested party

        // keep track of consumed raw data from the server
        var remainingLength = msg.numBytes
        while (remainingLength > 0) {
          receivedQueue(0) match {
            case bs if bs.length <= remainingLength =>
              // the first ByteString was consumed completely --> drop it
              receivedQueue.remove(0)
              remainingLength -= bs.length
            case bs =>
              // the first ByteString was consumed partly --> slice it
              receivedQueue(0) = bs.drop(remainingLength)
              remainingLength = 0
          }
        }

        tryParse()
      case None =>
        // apparently the queue does not contain a complete response message
    }
  }

  private def trySend(): Unit = {
    if (sendQueue.nonEmpty && !isSending) {
      isSending = true

      val sendByteBuffer = sendQueue
        .tail
        .foldLeft(sendQueue.head)((r,it) => r ++ it)
        .toByteBuffer

      //TODO timeout

      //TODO tuning use channel.write(Array[ByteBuffer]
      channel.write(sendByteBuffer, "", new CompletionHandler[Integer, String] {
        override def failed (exc: Throwable, attachment: String) = {
          //TODO logging
          //TODO error handling
          println ("failed!!!")
          exc.printStackTrace ()

          self ! Failure (exc)
        }

        override def completed (result: Integer, attachment: String) = self ! DataSent(result)
      })
    }
  }

  private def onDataSent(numBytes: Int): Unit = {
    var remainingSentLength = numBytes
    while (remainingSentLength > 0) {
      sendQueue (0) match {
        case bs if bs.length <= remainingSentLength =>
          sendQueue.remove (0)
          remainingSentLength -= bs.length
        case bs =>
          sendQueue (0) = bs.drop (remainingSentLength)
          remainingSentLength = 0
      }
    }

    self ! TrySend
  }

  private def triggerRead(): Unit = {
    // we clear the ByteBuffer before every read (rather than immediately when data is available) te avoid accessing
    //  an actor's member from a non-actor thread.
    readBuffer.clear ()

    //TODO timeout
    channel.read(readBuffer, "", new CompletionHandler[Integer, String] {
      override def failed (exc: Throwable, attachment: String) = {
        //TODO logging
        //TODO error handling
        println ("failed!!!")
        exc.printStackTrace ()

        self ! Failure (exc)
      }

      override def completed (result: Integer, attachment: String) = {
        self ! RawDataFromServer (ByteString.fromArray (readBuffer.array (), 0, readBuffer.position ()))
        self ! TriggerRead
      }
    })
  }
}
