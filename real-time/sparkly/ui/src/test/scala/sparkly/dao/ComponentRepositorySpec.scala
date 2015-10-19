package sparkly.dao

import org.scalatest._

import sparkly.dao._
import sparkly.testing._

class ComponentRepositorySpec extends FlatSpec with Matchers {

  val componentBasePackage = "sparkly.testing"

  val mockStreamMetadata = new MockStream().metadata
  val testComponentMetadata = new TestComponent().metadata
  val testClassifierMetadata = new TestClassifier().metadata

  "Component repository" should "load component's metadata" in {
    val repository = new ComponentRepository(componentBasePackage)

    repository.components() should contain only (
      "sparkly.testing.MockStream" -> mockStreamMetadata,
      "sparkly.testing.TestClassifier" -> testClassifierMetadata,
      "sparkly.testing.TestComponent" -> testComponentMetadata
    )
  }

  "Component repository" should "load component metadata" in {
    val repository = new ComponentRepository(componentBasePackage)
    repository.component("sparkly.testing.TestClassifier").get should equal(testClassifierMetadata)
  }
}
