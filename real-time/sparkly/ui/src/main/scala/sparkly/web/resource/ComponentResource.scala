package sparkly.web.resource

import sparkly.dao.ComponentRepository
import sparkly.web.model.ModelMapper

class ComponentResource(
  implicit val componentRepository: ComponentRepository,
  implicit val modelMapper: ModelMapper) extends BaseResource {

  get("/") {
    componentRepository
      .components()
      .map{case (id, metadata) => (id, modelMapper.convert(id, metadata))}
  }

  get("/:id") {
    val id = params("id")
    componentRepository.component(id) match {
      case Some(metadata) => modelMapper.convert(id, metadata)
      case None => halt(404)
    }
  }
}
