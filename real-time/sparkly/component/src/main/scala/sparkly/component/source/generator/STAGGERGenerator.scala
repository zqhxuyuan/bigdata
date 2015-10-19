package sparkly.component.source.generator

import moa.streams.InstanceStream
import sparkly.core.FeatureType.FeatureType
import sparkly.core.PropertyType.INTEGER
import sparkly.core._
import scala.util.Random

class STAGGERGenerator extends MoaFixedFeatureGenerator {

  override def metadata = super.metadata.copy (
    name = "STAGGER generator",
    description = "Stream generator for STAGGER Concept functions. Generator described in the paper:\nJeffrey C. Schlimmer and Richard H. Granger Jr. \"Incremental Learning from Noisy Data\", Machine Learning 1: 317-354 1986.\n\nNotes:\nThe built in functions are based on the paper (page 341).",
    properties = super.metadata.properties ++ Map (
      "Classification function used" -> PropertyMetadata(INTEGER, defaultValue = Some(1), acceptedValues = (1 to 3).map(_.toString).toList)
    )
  )

  def features: List[(String, FeatureType)] = List (
    ("size", FeatureType.STRING),
    ("color", FeatureType.STRING),
    ("shape", FeatureType.STRING),
    ("class", FeatureType.STRING)
  )

  def generator(context: Context): InstanceStream = {
    val function = context.property("Classification function used").as[Int]

    val generator = new moa.streams.generators.STAGGERGenerator()
    generator.functionOption.setValue(function)
    generator.instanceRandomSeedOption.setValue(Random.nextInt)
    generator.prepareForUse()
    generator.restart()

    generator
  }
}