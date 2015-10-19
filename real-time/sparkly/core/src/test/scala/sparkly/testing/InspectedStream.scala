package sparkly.testing

import org.apache.spark.streaming.dstream.DStream
import sparkly.core.Instance

import scala.collection.mutable.ListBuffer

case class InspectedStream(instances: ListBuffer[Instance]) {
  def features() = instances.map(_.rawFeatures)
  def size() = instances.size
}


object InspectedStream {

  def apply(dstream: DStream[Instance]): InspectedStream = {
    val instances = ListBuffer[Instance]()
    dstream.foreachRDD { rdd => instances ++= rdd.collect()}

    InspectedStream(instances)
  }

  def inspect(dstream: DStream[Instance]) = InspectedStream(dstream).instances.map(_.rawFeatures)
}
