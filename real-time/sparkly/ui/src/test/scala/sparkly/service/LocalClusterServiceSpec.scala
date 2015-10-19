package sparkly.service

import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.mock.MockitoSugar

import sparkly.component.source.CsvFileDirectorySource
import sparkly.core._
import sparkly.dao._
import sparkly.config.SparklyConfig._
import sparkly.component.debug.Log
import sparkly.service.ClusterState._
import scala.reflect.io.Directory
import sparkly.component.source.dataset.SpamDataset

class LocalClusterServiceSpec extends FlatSpec with Matchers with MockitoSugar with BeforeAndAfterEach {

  implicit val pipelineRepository = mock[PipelineRepository]
  implicit val pipelineValidationService = mock[PipelineValidationService]
  val localClusterService: LocalClusterService = new LocalClusterService()

  override def beforeEach(): Unit = {
    Directory(BASE_DISTRIBUTED_DIRECTORY).deleteRecursively()
  }

  override def afterEach(): Unit = {
    localClusterService.stop(false)
    Directory(BASE_DISTRIBUTED_DIRECTORY).deleteRecursively()
  }

  "Local cluster" should "deploy pipeline" in {
    // Given
    when(pipelineRepository.get("pipeline1")).thenReturn(Some(pipeline))
    when(pipelineValidationService.validate(pipeline)).thenReturn(ValidationReport())

    // When
    localClusterService.deploy("pipeline1", true, false)

    // Then
    val state = localClusterService.status
    state.action should be (Running)
    state.time should not be empty
    state.pipeline should be (Some(pipeline))
    localClusterService.streamingContext should not be empty
  }

  "Local cluster" should "not deploy pipeline when not valid" in {
    // Given
    when(pipelineRepository.get("pipeline1")).thenReturn(Some(pipeline))
    when(pipelineValidationService.validate(pipeline)).thenReturn(ValidationReport(List(ValidationMessage("", MessageLevel.Error))))

    // When
    intercept[IllegalArgumentException] {
      localClusterService.deploy("pipeline1", true, false)
    }

    // Then
    val state = localClusterService.status
    state.action should be (Stopped)
  }

  "Local cluster" should "redeploy pipeline" in {
    // Given
    when(pipelineRepository.get("pipeline1")).thenReturn(Some(pipeline))
    when(pipelineValidationService.validate(pipeline)).thenReturn(ValidationReport())
    localClusterService.deploy("pipeline1", true, false)

    // When
    localClusterService.deploy("pipeline1", true, false)

    // Then
    val state = localClusterService.status
    state.action should be (Running)
    state.time should not be empty
    state.pipeline should be (Some(pipeline))
    localClusterService.streamingContext should not be empty
  }

  val pipeline = PipelineConfiguration (
    name = "test",
    components = List (
      ComponentConfiguration (
        id = "spam_source",
        name = "Train data",
        clazz = classOf[SpamDataset].getName,
        outputs = Map(
          "Instances" -> StreamConfiguration(mappedFeatures = Map("Label" -> "Label", "Features" -> "Features"))
        )
      ),
      ComponentConfiguration (
        id = "debug",
        name = "Debug",
        clazz = classOf[Log].getName,
        inputs = Map(
          "Input" -> StreamConfiguration(selectedFeatures = Map("Features" -> List("Label")))
        )
      )
    ),
    connections = List(
      ConnectionConfiguration("spam_source", "Instances", "debug", "Input")
    )
  )
}

