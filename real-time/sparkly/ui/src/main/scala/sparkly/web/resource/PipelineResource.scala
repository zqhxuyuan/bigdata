package sparkly.web.resource

import sparkly.core.PipelineConfiguration
import sparkly.dao.PipelineRepository
import sparkly.web.model._

class PipelineResource(
  implicit val pipelineRepository: PipelineRepository,
  implicit val modelMapper: ModelMapper) extends BaseResource {

  get("/") {
    pipelineRepository
      .all()
      .map(modelMapper.convert(_))
  }

  get("/:id") {
    val id = params("id")
    pipelineRepository.get(id) match {
      case Some(pipeline) => modelMapper.convert(pipeline)
      case None => halt(404)
    }
  }

  delete("/:id") {
    val id = params("id")
    pipelineRepository.delete(id)
  }

  post("/:name") {
    val name = params("name")
    val pipeline = PipelineConfiguration(name = name)
    pipelineRepository.store(pipeline.id, pipeline)
    modelMapper.convert(pipeline)
  }

  put("/:id") {
    val id = params("id")
    val pipeline = parsedBody.extract[PipelineConfigurationModel]
    require(pipeline.id == id)
    pipelineRepository.store(id, modelMapper.convert(pipeline))
  }

}