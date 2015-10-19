package sparkly.service

import sparkly.core._
import sparkly.dao._
import sparkly.service.MessageLevel.MessageLevel

import scala.util.Try

class PipelineValidationService(implicit val componentRepository: ComponentRepository) {

  val componentValidator = new ComponentValidator()

  def validate(pipeline: PipelineConfiguration): ValidationReport = {
    val componentsMessages = pipeline.components.flatMap(componentValidator.validate)
    ValidationReport(componentsMessages)
  }
}

class ComponentValidator(implicit val componentRepository: ComponentRepository) {
  def validate(componentConfiguration: ComponentConfiguration): List[ValidationMessage] = {
    val componentName = componentConfiguration.name

    val componentMetadata = componentRepository.component(componentConfiguration.clazz) match {
      case Some(metadata) => metadata
      case None => {
        return List(ValidationMessage(s"'${componentName}' component implementation '${componentConfiguration.clazz}' is unknown.", MessageLevel.Error))
      }
    }

    val missingPropertiesReport = componentMetadata.properties
      .filter{case (name, metadata) => metadata.mandatory && metadata.defaultValue.isEmpty && !componentConfiguration.propertyExists(name)}
      .map{case (name, metadata) => ValidationMessage(s"'${name}' property of '${componentName}' component is mandatory.", MessageLevel.Error)}

    val badPropertyValuesReport = componentMetadata.properties
      .map{case (name, metadata) => (name, metadata, componentConfiguration.properties.get(name))}
      .filter{case (name, metadata, value) =>
        val property = Property(metadata, value)
        Try(property.get).isFailure
      }
      .map{case (name, metadata, value) => ValidationMessage(s"'${name}' property of '${componentName}' component is a ${metadata.propertyType} property. Value '${value.getOrElse(null)}' is not supported.", MessageLevel.Error)}

    val missingInputMappings = componentMetadata.inputs
      .flatMap{case (name, metadata) =>
        val mappedFeatures = componentConfiguration.inputs.getOrElse(name, StreamConfiguration()).mappedFeatures.keys.toList
        metadata.namedFeatures.keys.filter(!mappedFeatures.contains(_)).map(missingMapping => (name, metadata, missingMapping))
      }.map{case (name, metadata, missingMapping) =>
        ValidationMessage(s"Missing mapping '${missingMapping}' of input '${name}' in '${componentName}' component.", MessageLevel.Error)
      }

    val missingOutputMappings = componentMetadata.outputs
      .flatMap{case (name, metadata) =>
        val mappedFeatures = componentConfiguration.outputs.getOrElse(name, StreamConfiguration()).mappedFeatures.keys.toList
        metadata.namedFeatures.keys.filter(!mappedFeatures.contains(_)).map(missingMapping => (name, metadata, missingMapping))
      }.map{case (name, metadata, missingMapping) =>
        ValidationMessage(s"Missing mapping '${missingMapping}' of output '${name}' in '${componentName}' component.", MessageLevel.Error)
      }

    missingPropertiesReport.toList ++ badPropertyValuesReport ++ missingInputMappings ++ missingOutputMappings
  }
}

case class ValidationReport(messages: List[ValidationMessage] = List()) {
  def add(message: ValidationMessage): ValidationReport = this.copy(messages = this.messages :+ message)
  def add(text: String, level: MessageLevel): ValidationReport = add(ValidationMessage(text, level))

  def containsErrors() = messages.find(_.level == MessageLevel.Error).isDefined
}

case class ValidationMessage(text: String, level: MessageLevel)

object MessageLevel extends Enumeration {
  type MessageLevel = Value
  val Warning, Error = Value
}