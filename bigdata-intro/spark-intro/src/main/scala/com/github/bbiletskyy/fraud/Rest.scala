package com.github.bbiletskyy.fraud

/**
 * Created by zhengqh on 15/10/12.
 */
import akka.actor.{ ActorSystem, Props }
import akka.io.IO
import spray.can.Http
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.Await
import scala.concurrent.duration._

/** Object in charge of REST interface startup. */
object Rest {
  /** Launches a http servers and redirects http messages to the  receiver actor.*/
  def init(actorSystem: ActorSystem, driverHost: String, driverPort: Int, receiverActorName: String) {
    implicit val system = actorSystem
    val connector = lookup(actorSystem, driverHost, driverPort, receiverActorName)
    val service = system.actorOf(Props(new RestServiceActor(connector)), "rest-service")

    implicit val timeout = Timeout(5.seconds)
    IO(Http) ? Http.Bind(service, interface = "localhost", port = 8080)
  }

  /** Looks up a receiver actor.*/
  def lookup(actorSystem: ActorSystem, host: String, port: Int, actorName: String) = {
    import scala.concurrent.duration._
    val url = s"akka.tcp://sparkDriver@$host:$port/user/Supervisor0/$actorName"
    val timeout = 10.seconds
    val receiver = Await.result(actorSystem.actorSelection(url).resolveOne(timeout), timeout)
    receiver

  }
}
