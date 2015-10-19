package sparkly.service

import java.util.Date

import org.apache.spark.streaming._
import sparkly.config.SparklyConfig._
import sparkly.core._
import sparkly.dao.PipelineRepository

class LocalClusterService(
  implicit val pipelineValidationService: PipelineValidationService,
  implicit val pipelineRepository: PipelineRepository) {

  val streamingContextFactory = new StreamingContextFactory(BASE_DISTRIBUTED_DIRECTORY, "local[16]", "local")
  var streamingContext: Option[StreamingContext] = None

  var status: ClusterStatus = ClusterStatus(ClusterState.Stopped, None, None)

  def deploy(pipelineId: String, restore: Boolean, stopGracefully: Boolean = true) {
    pipelineRepository.get(pipelineId) match {
      case None => throw new IllegalArgumentException("Pipeline with id " + pipelineId + " does not exists")
      case Some(pipeline) => {
        check(pipeline)
        stop(stopGracefully)
        start(pipeline, restore)
      }
    }
  }

  def check(pipeline: PipelineConfiguration): Unit = {
    val report = pipelineValidationService.validate(pipeline)
    if(report.containsErrors) {
      throw new IllegalArgumentException(s"Pipeline with id ${pipeline.id} is not valid. Validation report : \n${report}")
    }
  }

  def stop(stopGracefully: Boolean) {
    status = ClusterStatus(ClusterState.Stopping, None, None)

    streamingContext.foreach{ ssc =>
      try {
        ssc.stop(stopSparkContext = true, stopGracefully = stopGracefully)
        ssc.awaitTerminationOrTimeout(5000)
        streamingContext = None
      } catch {
        case e: Exception => println("Unable to stop streaming context")  // TODO : should log!!
      }
    }

    status = ClusterStatus(ClusterState.Stopped, None, None)
  }

  def start(pipeline: PipelineConfiguration, restore: Boolean) {
    status = ClusterStatus(ClusterState.Deploying, Some(new Date()), Some(pipeline))

    try {
      val ssc = if(restore) {
        streamingContextFactory.restoreStreamingContext(pipeline)
      } else {
        streamingContextFactory.createStreamingContext(pipeline)._1
      }

      streamingContext = Some(ssc)
      streamingContext.get.start()

      status = ClusterStatus(ClusterState.Running, Some(new Date()), Some(pipeline))
    } catch {
      case e: Exception => {
        stop(false)
        // TODO : should log!!
        throw new IllegalArgumentException(s"Unable to start pipeline ${pipeline.id}", e)
      }
    }
  }

}

case class ClusterStatus(action: ClusterState.ClusterAction, time: Option[Date], pipeline: Option[PipelineConfiguration]) {
  def isReadyToDeploy() = action match {
    case ClusterState.Stopped => true
    case ClusterState.Running => true
    case _ => false
  }
}

object ClusterState extends Enumeration {
  type ClusterAction = Value
  val Stopped, Deploying, Running, Stopping = Value
}
