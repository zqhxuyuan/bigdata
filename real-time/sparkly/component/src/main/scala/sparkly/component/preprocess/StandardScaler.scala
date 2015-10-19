package sparkly.component.preprocess

import org.apache.spark.mllib.linalg.VectorUtil._
import org.apache.spark.streaming.dstream.DStream
import sparkly.math.feature.Standardizer
import scala.util.Try
import scala.Some
import sparkly.core.FeatureType.VECTOR
import sparkly.core._
import sparkly.math._
import sparkly.core.PropertyType._

class StandardScaler extends Component {

  def metadata = ComponentMetadata(
    name = "Standard scaler", category = "Pre-processor",
    description = "Standardize continuous features by removing the mean and scaling to unit variance",
    inputs = Map (
      "Input" -> InputStreamMetadata(namedFeatures = Map("Features" -> VECTOR))
    ), outputs = Map (
      "Output" -> OutputStreamMetadata(from = Some("Input"), namedFeatures = Map("Standardized features" -> VECTOR))
    ), properties = Map (
      "Parallelism" -> PropertyMetadata(INTEGER, defaultValue = Some(-1), description = "Level of parallelism to use. -1 to avoid repartitioning.")
    )
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val partitions = context.property("Parallelism").as[Int]
    val stream = if(partitions < 1) context.dstream("Input", "Output") else context.dstream("Input", "Output").repartition(partitions)

    var size: Int = -1
    var standardizer: Standardizer = null

    val features = stream.map(i => i.inputFeature("Features").asVector)

    // Init if needed
    features.foreachRDD{ rdd => if(standardizer == null && !rdd.isEmpty) {
      size = rdd.first().length
      standardizer = Standardizer(size)
    }}

    features
      .mapPartitions( data => if(data.isEmpty) Iterator() else Iterator(Standardizer(size, data)))
      .reduce(_ + _)
      .foreachRDD{ rdd =>
        Try(rdd.take(1)(0)).foreach{ update =>
          standardizer = standardizer + update
        }
      }

    val out = stream.map{ instance =>
      val features = standardizer.standardize(instance.inputFeature("Features").asVector.toDenseBreeze)
      instance.outputFeature("Standardized features", features)
    }

    Map("Output" -> out)
  }

}