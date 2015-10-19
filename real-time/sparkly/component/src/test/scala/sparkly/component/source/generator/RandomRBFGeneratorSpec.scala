package sparkly.component.source.generator

import sparkly.testing._
import sparkly.core._

class RandomRBFGeneratorSpec extends ComponentSpec {

  "Random RBF generator" should "generate random features" in {
    val configuration = ComponentConfiguration (
      clazz = classOf[RandomRBFGenerator].getName,
      name = "Source",
      outputs = Map (
        "Instances" -> StreamConfiguration(selectedFeatures = Map ("Features" -> List ("f1", "f2", "f3", "f4", "f5")), mappedFeatures = Map("Class" -> "class"))
      )
    )

    val component = deployComponent(configuration)

    eventually {
      component.outputs("Instances").instances.foreach{ instance =>
        instance.outputFeatures("Features").asList.exists(_.isEmpty) should be(false)
        instance.outputFeature("Class").asString should startWith("class")
      }

      // Check it generates random instances
      component.outputs("Instances").instances.toSet.size should be > 1
    }
  }

}
