package com.ajjpj.cassdriver.util

import akka.actor.Actor


trait CassLogger {
  def error(exc: Throwable, msg: => String)
  def error(msg: => String)
  def error(exc: Throwable)

  def warn(exc: Throwable, msg: => String)
  def warn(msg: => String)
  def warn(exc: Throwable)

  def info(exc: Throwable, msg: => String)
  def info(msg: => String)
  def info(exc: Throwable)

  def debug(exc: Throwable, msg: => String)
  def debug(msg: => String)
  def debug(exc: Throwable)
}
object CassLogger {
  def apply(cls: Class[_]) = new StdOutLogger(cls) //TODO logger implementations, lookup, ...
}

class StdOutLogger(cls: Class[_]) extends CassLogger {
  override def error (exc: Throwable, msg: => String) = log("ERROR", exc, msg)
  override def error (msg: => String): Unit = log("ERROR", msg)
  override def error (exc: Throwable): Unit = log("ERROR", exc)

  override def warn (exc: Throwable, msg: => String) = log("WARN ", exc, msg)
  override def warn (msg: => String): Unit = log("WARN ", msg)
  override def warn (exc: Throwable): Unit = log("WARN ", exc)

  override def info (exc: Throwable, msg: => String) = log("INFO ", exc, msg)
  override def info (msg: => String): Unit = log("INFO ", msg)
  override def info (exc: Throwable): Unit = log("INFO ", exc)

  override def debug (exc: Throwable, msg: => String) = log("DEBUG", exc, msg)
  override def debug (msg: => String): Unit = log("DEBUG", msg)
  override def debug (exc: Throwable): Unit = log("DEBUG", exc)


  private def log(severity: String, exc: Throwable, msg: => String): Unit = {
    log(severity, msg)
    log(severity, exc)
  }
  private def log(severity: String, msg: => String): Unit = {
    println (s"$severity $msg")
  }
  private def log(severity: String, exc: Throwable): Unit = {
    print (s"$severity ")
    exc.printStackTrace(System.out)
  }
}


trait CassLogging {
  self => Actor

  val log = CassLogger(getClass)
}
