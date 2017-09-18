package com.ajjpj.cassdriver.connection

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousSocketChannel, CompletionHandler}
import java.util.concurrent.TimeUnit

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorRef}
import akka.util.ByteString
import com.ajjpj.cassdriver.connection.api.QueryRequest
import com.ajjpj.cassdriver.connection.protocol_v4.ProtocolV4
import com.ajjpj.cassdriver.util.CassLogging

import scala.annotation.tailrec
import scala.collection.mutable


object AsyncSocketChannelCassandraConnection {

  //TODO move 'startup' handshake into the connection
  //TODO tuning: make the connection available immediately, with topology information etc. as a later optimization --> faster startup
  case class StartupRequest(cqlVersion: String) //TODO compression, tracing

  case object Initialized
  private case object TrySend
  private case class DataSent(numBytes: Long)
  private case object TriggerRead
  private case class RawDataFromServer (data: ByteString)
}

class AsyncSocketChannelCassandraConnection (config: CassandraConnectionConfig, owner: ActorRef) extends Actor with CassLogging {
  import AsyncSocketChannelCassandraConnection._

  //TODO performance logging, performance monitoring API

  //TODO socket options
  val channel = AsynchronousSocketChannel.open()
  channel.connect(new InetSocketAddress(config.address, config.port), "", new CompletionHandler[Void, String] {
    override def failed (exc: Throwable, attachment: String) = self ! Failure(exc)
    override def completed (result: Void, attachment: String) = self ! Initialized
  })

  val readBuffer = ByteBuffer.allocate(16384) //TODO tuning measure if allocateDirect improves performance

  /**
    * raw snippets sent from the server, stored in chronological order. Presence of more than one item may be due
    *  to back logging, or because a single Response spans several chunks
    */
  val receivedQueue = mutable.ArrayBuffer.empty[ByteString]

  /**
    * Send frames are queued so that send operations don't overlap: a frame is sent only after the previous frame
    *  was acknowledged by the network API. This is necessary because async channels do not guarantee to actually send
    *  all data passed to a send call (chunked send with a new API call required for each chunk).
    *
    * This has the desirable side effect of automatically batching several (small) frames in high-load scenarios, and it
    *  lays the foundation for explicit batching TODO tuning if explicit batching is even desirable
    */
  val sendQueue = mutable.ArrayBuffer.empty[ByteString]
  var isSending = false

  private var curStreamNumber = 0
  def nextStreamNumber = {
    do {
      curStreamNumber = (curStreamNumber + 1) & 0x7fff
    }
    while (inFlight contains curStreamNumber) // guard against wrap-around
    curStreamNumber
  }

  private case class InFlightData(replyTo: ActorRef)
  private val inFlight = mutable.Map.empty[Int, InFlightData]


  val initializing: Receive = {
    case Initialized =>
      //noinspection ForwardReference
      context.become(processing)
      owner ! Initialized
      self ! TriggerRead
    case msg@Failure(th) =>
      log.error(th, "error initializing connection - terminating connection")
      context.stop(self)
  }

  val processing: Receive = {
    case StartupRequest(cqlVersion) =>
      onStartupRequest(cqlVersion)
    case msg: QueryRequest =>
      onQueryRequest(msg)

    case TrySend =>
      trySend()
    case DataSent(numBytes) =>
      onDataSent(numBytes)

    case TriggerRead =>
      triggerRead()

    case RawDataFromServer(bs) =>
      receivedQueue += bs
      tryParse()

    case msg@Failure(th) =>
      log.error(th, "error in connection - terminating connection")
      context.stop(self)
  }

  override def receive = initializing

  private def onStartupRequest(cqlVersion: String): Unit = {
    // Serializing requests in the connection actor creates some overhead and may reduce a single connection's
    //  throughput (pending actual measurements). Our design however requires parsing the responses on the
    //  actor's thread, which can incur significantly more overhead (think large result sets), and throughput
    //  can be increased by having several connections, so we consider that to be a good trade-off. TODO move this documentation to a more prominent place
    registerAndSend (ProtocolV4.createStartupMessage (nextStreamNumber, cqlVersion))
  }

  private def onQueryRequest(msg: QueryRequest): Unit = {
    registerAndSend (ProtocolV4.createQueryMessage(nextStreamNumber, msg))
  }

  private def registerAndSend(request: ByteString): Unit = {
    inFlight += curStreamNumber -> InFlightData(sender)
    sendQueue += request
    trySend()
  }

  @tailrec
  private def tryParse(): Unit = {
    ProtocolV4.parseResponse (receivedQueue, 0) match {
      case Some(msg) =>
        inFlight.get (msg.stream) match {
          case Some(InFlightData(replyTo)) =>
            replyTo ! msg
          case None =>
            log.warn(s"received out-of-band data: $msg")
        }
        inFlight -= msg.stream

        // remove consumed raw data
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
        // the queue does not contain a complete response message
    }
  }

  private def trySend(): Unit = {
    log.debug("*** try send")

    if (sendQueue.nonEmpty && !isSending) {
      isSending = true

      log.debug("*** sending")

      val sendByteBuffers = sendQueue
        .tail
        .foldLeft(sendQueue.head)((r,it) => r ++ it)
        .asByteBuffers
        .toArray

      //TODO timeout
      //TODO tuning use channel.write(Array[ByteBuffer]

      channel.write(sendByteBuffers, 0, sendByteBuffers.length, 0, TimeUnit.SECONDS, "", new CompletionHandler[java.lang.Long, String] {
        override def failed (exc: Throwable, attachment: String)     = {
          log.debug(exc, "error sending")
          self ! Failure (exc)
        }
        override def completed (result: java.lang.Long, attachment: String) = {
          log.debug("*** send completed")
          self ! DataSent(result)
        }
      })
    }
  }

  private def onDataSent(numBytes: Long): Unit = {
    isSending = false

    var remainingSentLength = numBytes
    while (remainingSentLength > 0) {
      sendQueue (0) match {
        case bs if bs.length <= remainingSentLength =>
          sendQueue.remove (0)
          remainingSentLength -= bs.length
        case bs =>
          sendQueue (0) = bs.drop (remainingSentLength.toInt)
          remainingSentLength = 0
      }
    }

    self ! TrySend
  }

  private def triggerRead(): Unit = {
    // we clear the ByteBuffer before every read (rather than immediately when data is available) te avoid accessing
    //  an actor's member from a non-actor thread.
    readBuffer.clear ()

    log.debug("*** trigger read")

    //TODO timeout
    channel.read(readBuffer, "", new CompletionHandler[Integer, String] {
      override def failed (exc: Throwable, attachment: String) = {
        log.debug(exc, "error receiving")
        self ! Failure (exc)
      }

      override def completed (result: Integer, attachment: String) = {
        log.debug("*** read completed")

        // These messages must arrive in the same order they were sent, and fortunately Akka guarantees that
        self ! RawDataFromServer (ByteString.fromArray (readBuffer.array (), 0, readBuffer.position ()))
        self ! TriggerRead
      }
    })
  }
}
