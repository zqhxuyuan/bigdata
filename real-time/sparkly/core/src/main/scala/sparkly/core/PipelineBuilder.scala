package sparkly.core

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable
import scala.collection.mutable.{Map => MutableMap}

class PipelineBuilder {

  def build(ssc: StreamingContext, pipelineDirectory: String, pipeline: PipelineConfiguration): BuildResult = {
    val connections = pipeline.connections
    implicit val componentOrdering = ComponentOrdering(pipeline)

    val outputStreams = MutableMap[(String, String), DStream[Instance]]();
    val components = MutableMap[String, Component]();

    pipeline.components.sorted
      .foreach{componentConfiguration =>
        val componentId = componentConfiguration.id

        val component = Class.forName(componentConfiguration.clazz).newInstance.asInstanceOf[Component]
        val inputs: Map[String, DStream[Instance]] = component.metadata.inputs.keys
          .map { name =>
            val dstream = connections.find(_.isTo(componentId, name)).flatMap(conn => outputStreams.get((conn.from.component, conn.from.stream)))
            (name, dstream.getOrElse(emptyStream(ssc)))
          }.toMap

        val outputs = component.init(ssc, pipelineDirectory, pipeline.id, componentConfiguration, inputs)
        outputStreams ++= outputs.map(t => (componentConfiguration.id, t._1) -> t._2)
      components += componentId -> component
      }

    BuildResult(components.toMap, outputStreams.toMap)
  }

  private def emptyStream(ssc: StreamingContext): DStream[Instance] = {
    val queue = new mutable.SynchronizedQueue[RDD[Instance]]()
    ssc.queueStream(queue)
  }

}

case class ComponentOrdering(configuration: PipelineConfiguration) extends Ordering[ComponentConfiguration] {
  val indexes = MutableMap[String, Int]()

  def compare(c1: ComponentConfiguration, c2: ComponentConfiguration): Int =  {
    indexOf(c1.id) - indexOf(c2.id)
  }

  def indexOf(componentId: String): Int = indexes.get(componentId) match {
    case Some(index) => index
    case None => {
      val inputIndexes = configuration.connections
        .filter(_.to.component == componentId)
        .map(connection => indexOf(connection.from.component))

      val index = inputIndexes.fold(0)(_+_) + 1
      indexes += componentId -> index
      index
    }
  }
}

case class BuildResult(components: Map[String, Component], outputs: Map[(String, String), DStream[Instance]])