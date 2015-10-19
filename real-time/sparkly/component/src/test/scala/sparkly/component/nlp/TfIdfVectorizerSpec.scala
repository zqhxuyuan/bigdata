package sparkly.component.nlp

import sparkly.testing._
import sparkly.core._
import sparkly.component.source.dataset.ReutersDataset

class TfIdfVectorizerSpec extends ComponentSpec {

  "TfIdfVectorizer" should "create tf-idf features" in {
    // Given
    val configuration = ComponentConfiguration (
      clazz = classOf[TfIdfVectorizer].getName,
      name = "TfIdfVectorizer",
      inputs = Map ("Input" -> StreamConfiguration(mappedFeatures = Map("Text" -> "Text"))),
      outputs = Map ("Output" -> StreamConfiguration(mappedFeatures = Map("Features" -> "Features"))),
      properties = Map ("Vocabulary size" -> "500")
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("Input").push(5000, ReutersDataset.iterator())

    // Then
    eventually {
      component.outputs("Output").instances should have size (2215)
      component.outputs("Output").instances.map(_.rawFeature("Features").asVector).foreach(vector => vector.size should be (500))
    }
  }

}
