package sparkly.component.misc

import sparkly.core._
import sparkly.core.PropertyType._
import org.apache.spark.streaming.dstream.DStream
import scala.Some
import breeze.linalg.DenseVector

class VectorCreator extends Component {

  override def metadata: ComponentMetadata = ComponentMetadata (
    name = "Vector creator", category = "Miscellaneous",
    description = "Create vectors",
    properties = Map(
      "Default value" -> PropertyMetadata(DECIMAL, defaultValue = Some(0.0))
    ),
    inputs = Map ("Input" -> InputStreamMetadata(listedFeatures = Map("Features" -> FeatureType.ANY))),
    outputs = Map("Output" -> OutputStreamMetadata(from = Some("Input"), namedFeatures = Map("Vector" -> FeatureType.VECTOR)))
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val default = context.property("Default value").as[Double]

    val out = context.dstream("Input", "Output").map { instance =>
      val features = instance.inputFeatures("Features").values

      val size = features.foldLeft(0)((current, f) => f match {
        case v: VectorFeature => current + v.asVector.length
        case _ => current + 1
      })

      val result = DenseVector.zeros[Double](size)
      var offset = 0

      for (f <- features) {
        f match {
          case v: VectorFeature => {
            val vector = v.asVector
            result.slice(offset, offset + vector.size) := vector
            offset += vector.size
          }
          case _ => {
            result(offset) = f.asDoubleOr(default)
            offset += 1
          }
        }
      }

      instance.outputFeature("Vector", result)
    }

    Map("Output" -> out)
  }

}
