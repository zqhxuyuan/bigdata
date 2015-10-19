package sparkly.web.resource

import com.google.common.collect.EvictingQueue
import org.json4s._
import org.scalatra.atmosphere.{JsonMessage, _}
import sparkly.web.model.ModelMapper

import scala.collection.mutable.{Map => MutableMap}
import scala.concurrent.ExecutionContext.Implicits.global

class MonitoringResource(implicit val modelMapper: ModelMapper) extends BaseResource with AtmosphereSupport {

  private val monitoringDataHistory: MutableMap[String, MonitoringDataQueue] = MutableMap()

  get("/past_data/:master/:id") {
    val master = params("master")
    val id = params("id")
    val monitoringId = master + ":" + id
    getMonitoringDataHistory(monitoringId).all()
  }

  atmosphere("/data/:master/:id") {
    val master = params("master")
    val id = params("id")
    val monitoringId = master + ":" + id
    val data = getMonitoringDataHistory(monitoringId)
    new MonitoringAtmosphereClient(monitoringId, data)
  }

  private def getMonitoringDataHistory(id: String) = monitoringDataHistory.get(id) match {
    case Some(data) => data
    case None => {
      val data = new MonitoringDataQueue()
      monitoringDataHistory.put(id, data)
      data
    }
  }

  class MonitoringAtmosphereClient(val id: String, val data: MonitoringDataQueue) extends AtmosphereClient {

    def receive: AtmoReceive = {
      case JsonMessage(json) => {
        data.add(json)
        broadcastEvent(json)
      }
      case _ =>
    }

    def broadcastEvent(msg: OutboundMessage) {
      try {
        broadcast(msg, CustomClientFilter)
      } catch {
        case e: NoSuchMethodError => // java.lang.NoSuchMethodError: akka.actor.ActorSystem.dispatcher()Lscala/concurrent/ExecutionContextExecutor;
        // But it should work!
      }
    }

    val CustomClientFilter = new ClientFilter(null) {
      def apply(client: AtmosphereClient) = client.asInstanceOf[MonitoringAtmosphereClient].id == id
      override def toString(): String = "CustomClientFilter"
    }
  }
}

class MonitoringDataQueue {
  import scala.collection.JavaConversions._

  private val MAX_DATA = 100
  private val queue = EvictingQueue.create[JValue](MAX_DATA)

  def add(json: JValue) = queue.offer(json)
  def all() = queue.toList
}