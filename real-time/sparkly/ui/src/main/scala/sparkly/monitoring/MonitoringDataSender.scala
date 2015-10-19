package sparkly.monitoring

import org.atmosphere.wasync.impl.{DefaultOptions, DefaultOptionsBuilder, DefaultRequestBuilder}
import org.atmosphere.wasync.{Socket, Client, ClientFactory, Request}

import org.json4s._
import org.json4s.jackson.Serialization

import scala.concurrent.Future
import sparkly.core.MonitoringData

class MonitoringDataSender(val hostname: String, val port: Int) {

  implicit val formats = DefaultFormats
  import scala.concurrent.ExecutionContext.Implicits.global

  def send(data: MonitoringData[_], clusterId: String, monitoringId: String): Unit = {
    val url = s"ws://${hostname}:${port}/api/monitoring/data/${clusterId}/${monitoringId}"
    val socket = SocketFactory.create(url)

    val json = Serialization.write(data)
    Future { socket.fire(json) }
  }

}

object SocketFactory {
  import scala.collection.mutable.Map

  private val client: Client[DefaultOptions, DefaultOptionsBuilder, DefaultRequestBuilder] = ClientFactory.getDefault.newClient.asInstanceOf[Client[DefaultOptions, DefaultOptionsBuilder, DefaultRequestBuilder]]
  private val opts = client.newOptionsBuilder().build()

  private val sockets: Map[String, Socket] = Map()
  sys.addShutdownHook{ sockets.map(_._2).foreach(_.close()) }

  def create(url: String): Socket = sockets.get(url) match {
    case Some(socket) => socket
    case None => {
      val req = client.newRequestBuilder.method(Request.METHOD.GET).uri(url).transport(Request.TRANSPORT.WEBSOCKET)
      val socket = client.create(opts).open(req.build())
      sockets += url -> socket
      socket
    }
  }
}
