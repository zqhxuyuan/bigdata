package sparkly.component.preprocess

import sparkly.testing.ComponentSpec
import sparkly.core.{Instance, StreamConfiguration, ComponentConfiguration}
import scala.util.Random
import breeze.linalg.DenseVector

class StandardScalerSpec extends ComponentSpec {

  "StandardScaler" should "standardize features" in {
    // Given
    val configuration = ComponentConfiguration (
      clazz = classOf[StandardScaler].getName,
      name = "StandardScaler",
      inputs = Map (
        "Input" -> StreamConfiguration(mappedFeatures = Map("Features" -> "Features"))
      ),
      outputs = Map (
        "Output" -> StreamConfiguration(mappedFeatures = Map("Standardized features" -> "Standardized features"))
      )
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("Input").push (1000, () => {
      Instance("Features" -> DenseVector(Array(Random.nextGaussian * 3.0 + 2.0, Random.nextGaussian * -1.5 + 1.0, Random.nextGaussian)))
    })

    // Then
    eventually {
      component.outputs("Output").features.size should be > 500
      val f1 = component.outputs("Output").features.map(data =>  data("Standardized features").asInstanceOf[DenseVector[Double]](0))
      val f2 = component.outputs("Output").features.map(data =>  data("Standardized features").asInstanceOf[DenseVector[Double]](1))
      val f3 = component.outputs("Output").features.map(data =>  data("Standardized features").asInstanceOf[DenseVector[Double]](2))

      mean(f1) should be (0.0 +- 0.1)
      stddev(f1) should be (1.0 +- 0.1)

      mean(f2) should be (0.0 +- 0.1)
      stddev(f2) should be (1.0 +- 0.1)

      mean(f3) should be (0.0 +- 0.1)
      stddev(f3) should be (1.0 +- 0.1)
    }
  }


  def stddev(values: Iterable[Double]): Double = {
    val mean = values.sum / values.size
    val devs = values.map(v => (v - mean) * (v - mean))
    Math.sqrt(devs.sum / values.size)
  }

  def mean(values: Iterable[Double]): Double = values.sum / values.size
}
