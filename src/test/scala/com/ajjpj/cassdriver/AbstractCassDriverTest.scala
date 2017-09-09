package com.ajjpj.cassdriver

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}


abstract class AbstractCassDriverTest extends TestKit(ActorSystem()) with FlatSpecLike with Matchers with ImplicitSender with BeforeAndAfterAll {

}
