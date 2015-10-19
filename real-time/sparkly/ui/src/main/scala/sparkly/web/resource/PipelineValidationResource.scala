package sparkly.web.resource

import sparkly.service.PipelineValidationService
import sparkly.web.model.{PipelineConfigurationModel, ModelMapper}

class PipelineValidationResource(
  implicit val pipelineValidationService: PipelineValidationService,
  implicit val modelMapper: ModelMapper) extends BaseResource {

  post("/") {
    val pipeline = modelMapper.convert(parsedBody.extract[PipelineConfigurationModel])
    val report = pipelineValidationService.validate(pipeline)
    modelMapper.convert(report)
  }
}
