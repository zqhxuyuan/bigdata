package sparkly.component.monitoring

import sparkly.core._
import sparkly.core.PropertyType._
import scala.util.Try
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming._

class ThroughputMonitoring extends Component {

  override def metadata = ComponentMetadata (
    name = "Throughput", category = "Monitoring",
    description = "Create throughput monitoring",
    properties = Map(
      "Window length (in ms)" -> PropertyMetadata(LONG),
      "Parallelism" -> PropertyMetadata(INTEGER, defaultValue = Some(-1), description = "Level of parallelism to use. -1 to use default level.")
    ),
    inputs = Map("In" -> InputStreamMetadata()),
    monitorings = Map("Throughput" -> MonitoringMetadata(ChartType.LINES, values = List("Throughput"), primaryValues = List("Throughput"), unit = "instance/s"))
  )

  override def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val dstream = context.dstream("In")
    val windowDuration = context.properties("Window length (in ms)").as[Long]
    val partitions = context.property("Parallelism").or(context.sc.defaultParallelism, on = (parallelism: Int) => parallelism < 1)
    val monitoring = context.createMonitoring[Map[String, Double]]("Throughput")

    dstream
      .repartition(partitions)
      .countByWindow(Milliseconds(windowDuration), dstream.slideDuration)
      .foreachRDD((rdd, time) => {
        val count = Try(rdd.take(1)(0)).toOption.getOrElse(0L)
        val throughput = 1000 * count / windowDuration.toDouble

        monitoring.add(time.milliseconds, Map("Throughput" -> throughput))
      })

    Map()
  }
}

