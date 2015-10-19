package sparkly.utils

import scala.reflect.io.Path

object SparklyDirectoryStructure {

  def getPipelineDirectory(baseDistributedDirectory: String, pipelineId: String): String = {
    Path(List(baseDistributedDirectory, pipelineId)).toString
  }

  def getCheckpointDirectory(pipelineDirectory: String, pipelineId: String): String = {
    Path(List(pipelineDirectory, "checkpoints")).toString
  }

  def getStateDirectory(pipelineDirectory: String, stateId: String): String = {
    Path(List(pipelineDirectory, "states", stateId)).toString
  }
}
