package sparkly.component.source.generator

import sparkly.testing._
import sparkly.core._

class STAGGERGeneratorSpec extends ComponentSpec {

  "STAGGER source" should "generate random things" in {
    val configuration = ComponentConfiguration (
      clazz = classOf[STAGGERGenerator].getName,
      name = "Source",
      outputs = Map (
        "Instances" -> StreamConfiguration(mappedFeatures = Map (
          "size" -> "size",
          "color" -> "color",
          "shape" -> "shape",
          "class" -> "class"
        ))
      )
    )

    val component = deployComponent(configuration)

    eventually {
      component.outputs("Instances").instances.foreach { instance =>
        instance.outputFeature("size").asString should (be("small") or be("medium") or be("large"))
        instance.outputFeature("color").asString should (be("green") or be("blue") or be("red"))
        instance.outputFeature("shape").asString should (be("circle") or be("triangle") or be("square"))
        instance.outputFeature("class").asString should (be("true") or be("false"))
      }

      // Check it generates random instances
      component.outputs("Instances").instances.toSet.size should be > 1
    }
  }

}
