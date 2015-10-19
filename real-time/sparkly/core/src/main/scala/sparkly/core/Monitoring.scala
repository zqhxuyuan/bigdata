package sparkly.core

import com.codahale.metrics.Gauge

class Monitoring[T](val name: String) extends Serializable {

  @volatile private var data: Option[MonitoringData[T]] = None

  def createMetric(): Gauge[Option[MonitoringData[T]]] = new Gauge[Option[MonitoringData[T]]] {
    override def getValue() = {
      Monitoring.this.clear()
    }
  }

  def add(data: T): Unit = add(System.currentTimeMillis, data)
  def add(timestamp: Long, data: T): Unit = this.data = Some(MonitoringData(timestamp, data))

  def clear(): Option[MonitoringData[T]] = {
    val result = data
    this.data = None
    result
  }

}

case class MonitoringData[T](timestamp: Long, data: T)

object Monitoring {
  def cleanName(name: String): String = name.replaceAll("[^A-Za-z0-9]", "-").toLowerCase
}