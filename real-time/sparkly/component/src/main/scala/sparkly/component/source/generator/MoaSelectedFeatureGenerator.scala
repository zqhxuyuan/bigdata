package sparkly.component.source.generator

import moa.streams.InstanceStream
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver
import sparkly.core.PropertyType.INTEGER
import sparkly.core._

abstract class MoaSelectedFeatureGenerator extends Component {

  def generator(context: Context): InstanceStream

  override def metadata = ComponentMetadata(
    name = "MOA generator",
    category = "Data Sources",
    outputs = Map("Instances" -> OutputStreamMetadata(namedFeatures = Map("Class" -> FeatureType.STRING), listedFeatures = Map("Features" -> FeatureType.DOUBLE))),
    properties = Map("Throughput (instance/second)" -> PropertyMetadata(INTEGER, defaultValue = Some(100)))
  )

  override def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val throughput = context.property("Throughput (instance/second)").as[Int]
    val instanceStream: InstanceStream = generator(context)
    val stream = context.ssc.receiverStream(new MoaSelectedFeatureGeneratorThread(throughput, instanceStream, context.outputMappers("Instances")))
    Map("Instances" -> stream)
  }

  class MoaSelectedFeatureGeneratorThread(throughput: Int, instanceStream: InstanceStream, outputMapper: Mapper) extends Receiver[Instance](StorageLevel.MEMORY_ONLY) with Logging {

    def onStart() {
      new Thread(s"${instanceStream.getClass} generator thread") {
        override def run() {
          generateInstance()
        }
      }.start()
    }

    def onStop() {
      // There is nothing much to do as the thread calling receive()
      // is designed to stop by itself isStopped() returns false
    }

    def toSparkly(instance: weka.core.Instance) = {
      val doubleFeatures: Map[String, Any] = outputMapper.featuresNames("Features").zipWithIndex.map{case (name, index) => (name, instance.value(index))}.toMap
      val classFeature = (outputMapper.featureName("Class") -> instance.stringValue(instance.classIndex))

      new Instance(doubleFeatures + classFeature, outputMapper = Some(outputMapper))
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