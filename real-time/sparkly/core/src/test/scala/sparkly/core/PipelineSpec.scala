package sparkly.core

import org.scalatest._

class PipelineSpec extends FlatSpec with Matchers {

  "Components" should "be ordered for creation" in {
    val configuration = PipelineConfiguration (
      name = "test",
      components = List (
        ComponentConfiguration(id = "d", name ="", clazz = ""),
        ComponentConfiguration(id = "a", name ="", clazz = ""),
        ComponentConfiguration(id = "b", name ="", clazz = ""),
        ComponentConfiguration(id = "c", name ="", clazz = ""),
        ComponentConfiguration(id = "e", name ="", clazz = "")
      ),
      connections = List (
        ConnectionConfiguration("a", "", "c", ""),
        ConnectionConfiguration("b", "", "c", ""),
        ConnectionConfiguration("b", "", "d", ""),
        ConnectionConfiguration("c", "", "e", ""),
        ConnectionConfiguration("d", "", "e", "")
      )
    )
    val componentOrdering = ComponentOrdering(configuration)
    val components: List[String] = configuration.components.sorted(componentOrdering).map(_.id)

    components.indexOf("a") should be < components.indexOf("c")
    components.indexOf("b") should be < components.indexOf("c")
    components.indexOf("b") should be < components.indexOf("d")
    components.indexOf("c") should be < components.indexOf("e")
    components.indexOf("d") should be < components.indexOf("e")
  }
}
