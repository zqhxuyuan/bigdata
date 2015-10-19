package sparkly.testing

import org.apache.spark.streaming.StreamingContext
import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.time._
import sparkly.core._

import scala.reflect.io.Directory
import scala.util.Try
import org.apache.spark.metrics.sink.MonitoringTestingData


object ComponentSpec {
  val batchDurationMs = 200
}

trait ComponentSpec extends FlatSpec with Matchers with BeforeAndAfterEach with Eventually {

  def patienceMs = 10000

  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(patienceMs, Milliseconds)), interval = scaled(Span(patienceMs / 100, Millis)))

  val cores = 8
  var pipelineDirectory = Directory.makeTemp("sparkly-component-test")
  val streamingContextFactory = new StreamingContextFactory(pipelineDirectory.toString(), s"local[$cores]", "test-cluster")

  var ssc: Option[StreamingContext] = None

  override def beforeEach() {
    super.beforeEach()
    pipelineDirectory.deleteRecursively()
  }

  override def afterEach() {
    super.afterEach()
    ssc.foreach{ssc =>
      try {
        ssc.stop()
        System.clearProperty("spark.driver.port")
        ssc.awaitTerminationOrTimeout(2000)
      } catch {
        case e: Exception => e.printStackTrace()// Use logger
      }
    }
    pipelineDirectory.deleteRecursively()
  }

  def deployComponent(componentConfiguration: ComponentConfiguration): RunningComponent = {
    val inputComponents = inputMockStream(componentConfiguration)
    val connections = inputComponents.map(inputComponent => ConnectionConfiguration(inputComponent._2.id, MockStream.OUTPUT_NAME, componentConfiguration.id, inputComponent._1)).toList
    val components = componentConfiguration :: inputComponents.values.toList

    val pipeline = PipelineConfiguration(name = this.getClass.getSimpleName, components = components, connections = connections, batchDurationMs = ComponentSpec.batchDurationMs)
    val (streamingContext, buildResult) = streamingContextFactory.createStreamingContext(pipeline)

    val mockedInputs = inputComponents.map{case(name, config) => // TODO ugly!
      val mockInput = buildResult.components.values.find(component => Try(component.asInstanceOf[MockStream].componentId == config.id).getOrElse(false))
      (name, mockInput.get.asInstanceOf[MockStream])
    }

    val inspectedOutputs = buildResult.outputs.map{case (names, dstream) => (names._2, InspectedStream(dstream))}

    ssc = Some(streamingContext)
    ssc.get.start()

    RunningComponent(componentConfiguration.id, mockedInputs, inspectedOutputs)
  }

  private def inputMockStream(component: ComponentConfiguration) = {
    ComponentMetadata.of(component).inputs.keys.map(name => (name, ComponentConfiguration(name = name, clazz = classOf[MockStream].getName))).toMap
  }

}

case class RunningComponent(componentId: String, inputs: Map[String, MockStream], outputs: Map[String, InspectedStream]) {
  def monitoringData[T](monitoring: String): List[MonitoringData[T]] = {
    val id = componentId + "-" + Monitoring.cleanName(monitoring)
    MonitoringTestingData.all[T](id)
  }

  def latestMonitoringData[T](monitoring: String): MonitoringData[T] = {
    val id = componentId + "-" + Monitoring.cleanName(monitoring)
    MonitoringTestingData.latest[T](id)
  }

}
