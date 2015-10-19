package sparkly.component.source.generator

import moa.streams.InstanceStream
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver
import sparkly.core.FeatureType.FeatureType
import sparkly.core.PropertyType.INTEGER
import sparkly.core._

abstract class MoaFixedFeatureGenerator extends Component {

  def features: List[(String, FeatureType)]

  def generator(context: Context): InstanceStream

  override def metadata = ComponentMetadata(
    name = "MOA generator",
    category = "Data Sources",
    outputs = Map("Instances" -> OutputStreamMetadata(namedFeatures = features.toMap)),
    properties = Map("Throughput (instance/second)" -> PropertyMetadata(INTEGER, defaultValue = Some(100)))
  )

  override def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val throughput = context.property("Throughput (instance/second)").as[Int]
    val instanceStream: InstanceStream = generator(context)
    val stream = context.ssc.receiverStream(new MoaGeneratorThread(throughput, instanceStream, context.outputMappers("Instances"), features))
    Map("Instances" -> stream)
  }

  class MoaGeneratorThread(throughput: Int, instanceStream: InstanceStream, outputMapper: Mapper, features: List[(String, FeatureType)]) extends Receiver[Instance](StorageLevel.MEMORY_ONLY) with Logging {

    def onStart() {
      new Thread(s"${instanceStream.getClass} generator thread") {
        override def run() {
          generateInstance()
        }
      }.start()
    }

    def onStop() {
    }

    def toSparkly(instance: weka.core.Instance) = {
      val actualFeatures = features.zipWithIndex.map { case ((name, featureType), index) => (name, featureType match {
        case FeatureType.DOUBLE => instance.value(index)
        case FeatureType.INTEGER => instance.value(index).toInt
        case FeatureType.LONG => instance.value(index).toLong
        case FeatureType.NUMBER => instance.value(index)
        case FeatureType.STRING => instance.stringValue(index)
      })
      }.toMap
      new Instance(actualFeatures, outputMapper = Some(outputMapper))
    }

    private def generateInstance() {
      while (!isStopped) {
        (1 to throughput).foreach { index =>
          val moaInstance = instanceStream.nextInstance()
          val instance: Instance = toSparkly(moaInstance)
          store(instance)
        }
        Thread.sleep(1000);
      }
    }
  }

}