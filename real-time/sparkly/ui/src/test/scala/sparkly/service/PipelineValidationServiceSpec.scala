package sparkly.service

import org.mockito.BDDMockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest._
import sparkly.core.FeatureType._
import sparkly.core.PropertyType._
import sparkly.core._
import sparkly.dao._

class PipelineValidationServiceSpec extends FlatSpec with Matchers with MockitoSugar {

  implicit val componentRepository = mock[ComponentRepository]
  val componentValidator = new ComponentValidator()

  "ComponentValidator" should "detect bad implementation class" in {
    // Given
    val configuration = ComponentConfiguration (name = "Test", clazz = "TestClass")
    given(componentRepository.component("TestClass")).willReturn(None)

    // When
    val messages = componentValidator.validate(configuration)

    // Then
    messages should contain (ValidationMessage("'Test' component implementation 'TestClass' is unknown.", MessageLevel.Error))
  }

  "ComponentValidator" should "detect missing property" in {
    // Given
    given(componentRepository.component("Test")).willReturn(Some(
      ComponentMetadata("Test",
        properties = Map (
          "Not mandatory" -> PropertyMetadata(DECIMAL, mandatory = false),
          "Mandatory" -> PropertyMetadata(DECIMAL, mandatory = true),
          "Mandatory 2" -> PropertyMetadata(DECIMAL, mandatory = true),
          "Mandatory 3" -> PropertyMetadata(DECIMAL, defaultValue = Some(0.25), mandatory = true)
        )
      )
    ))

    val configuration = ComponentConfiguration (name = "Test",
      clazz = "Test",
      properties = Map("Mandatory 2" -> null)
    )

    // When
    val messages = componentValidator.validate(configuration)

    // Then
    messages should contain only (
      ValidationMessage("'Mandatory' property of 'Test' component is mandatory.", MessageLevel.Error),
      ValidationMessage("'Mandatory 2' property of 'Test' component is mandatory.", MessageLevel.Error)
    )
  }

  "ComponentValidator" should "detect bad property value" in {
    // Given
    given(componentRepository.component("TestClass")).willReturn(Some(
      ComponentMetadata("TestClass",
        properties = Map (
          "Bias 1" -> PropertyMetadata(DECIMAL),
          "Bias 2" -> PropertyMetadata(DECIMAL)
        )
      )
    ))

    val configuration = ComponentConfiguration (name = "Test",
      clazz = "TestClass",
      properties = Map("Bias 1" -> "0.25", "Bias 2" -> "flute")
    )

    // When
    val messages = componentValidator.validate(configuration)

    // Then
    messages should contain only (ValidationMessage("'Bias 2' property of 'Test' component is a DECIMAL property. Value 'flute' is not supported.", MessageLevel.Error))
  }

  "ComponentValidator" should "detect missing mappings" in {
    // Given
    given(componentRepository.component("TestClass")).willReturn(Some(
      ComponentMetadata("TestClass",
        inputs = Map (
          "Input" -> InputStreamMetadata(namedFeatures = Map("Input Feature 1" -> ANY, "Input Feature 2" -> ANY))
        ),
        outputs = Map (
          "Output" -> OutputStreamMetadata(namedFeatures = Map("Output Feature 1" -> ANY, "Output Feature 2" -> ANY))
        )
      )
    ))

    val configuration = ComponentConfiguration (name = "Test", clazz = "TestClass",
      inputs = Map (
        "Input" -> StreamConfiguration(mappedFeatures = Map("Input Feature 1" -> "f1"))
      ),
      outputs = Map (
        "Output" -> StreamConfiguration(mappedFeatures = Map("Output Feature 2" -> "f4"))
      )
    )

    // When
    val messages = componentValidator.validate(configuration)

    // Then
    messages should contain only (
      ValidationMessage("Missing mapping 'Input Feature 2' of input 'Input' in 'Test' component.", MessageLevel.Error),
      ValidationMessage("Missing mapping 'Output Feature 1' of output 'Output' in 'Test' component.", MessageLevel.Error)
    )
  }

}
