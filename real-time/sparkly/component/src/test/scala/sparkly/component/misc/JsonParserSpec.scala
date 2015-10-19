package sparkly.component.misc

import sparkly.testing._
import sparkly.core._

class JsonParserSpec extends ComponentSpec {

  "JSON parser" should "extract fields" in {
    val configuration = ComponentConfiguration(
      name = "JSON parser", clazz = classOf[JsonParser].getName,
      properties = Map("Queries" -> "$.store.book[1].price;$.store.book[?(@.category == 'reference')].title;$.location"),
      inputs = Map ("Input" -> StreamConfiguration(mappedFeatures = Map("JSON" -> "raw-json"))),
      outputs = Map ("Output" -> StreamConfiguration(selectedFeatures = Map("Values" -> List("second book price", "reference title", "location"))))
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("Input").push(Instance("raw-json" -> rawJson))

    // Then
    eventually {
      component.outputs("Output").features should contain only {
        Map("raw-json" -> rawJson, "second book price" -> "12.99", "reference title" -> "Sayings of the Century", "location" -> null)
      }
    }
  }

  "JSON parser" should "extract json" in {
    val configuration = ComponentConfiguration(
      name = "JSON parser", clazz = classOf[JsonParser].getName,
      properties = Map("Queries" -> "$.store.book[0]"),
      inputs = Map ("Input" -> StreamConfiguration(mappedFeatures = Map("JSON" -> "raw-json"))),
      outputs = Map ("Output" -> StreamConfiguration(selectedFeatures = Map("Values" -> List("book json"))))
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("Input").push(Instance("raw-json" -> rawJson))

    // Then
    eventually {
      component.outputs("Output").features should contain only {
        Map("raw-json" -> rawJson, "book json" ->   """{"category":"reference","author":"Nigel Rees","title":"Sayings of the Century","price":8.95}""".stripMargin)
      }
    }
  }

  val rawJson =
    """
      |{
      |    "store": {
      |        "book": [
      |            {
      |                "category": "reference",
      |                "author": "Nigel Rees",
      |                "title": "Sayings of the Century",
      |                "price": 8.95
      |            },
      |            {
      |                "category": "fiction",
      |                "author": "Evelyn Waugh",
      |                "title": "Sword of Honour",
      |                "price": 12.99
      |            },
      |            {
      |                "category": "fiction",
      |                "author": "Herman Melville",
      |                "title": "Moby Dick",
      |                "isbn": "0-553-21311-3",
      |                "price": 8.99
      |            },
      |            {
      |                "category": "fiction",
      |                "author": "J. R. R. Tolkien",
      |                "title": "The Lord of the Rings",
      |                "isbn": "0-395-19395-8",
      |                "price": 22.99
      |            }
      |        ],
      |        "bicycle": {
      |            "color": "red",
      |            "price": 19.95
      |        }
      |    },
      |    "expensive": 10
      |}
    """.stripMargin
}
