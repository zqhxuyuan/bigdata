package sparkly.component.source.generator

import moa.streams.InstanceStream
import sparkly.core.PropertyType.INTEGER
import sparkly.core._
import scala.util.Random

class RandomRBFGenerator extends MoaSelectedFeatureGenerator {

  override def metadata = super.metadata.copy (
    name = "Random RBF generator",
    description = "Generates a random radial basis function stream.",
    properties = super.metadata.properties ++ Map (
      "Number of classes" -> PropertyMetadata(INTEGER, defaultValue = Some(2)),
      "Number of centroids in the model" -> PropertyMetadata(INTEGER, defaultValue = Some(50))
    )
  )

  def generator(context: Context): InstanceStream = {
    val numClasses = context.property( "Number of classes").as[Int]
    val numCentroids = context.property("Number of centroids in the model").as[Int]
    val featureCount = context.outputMappers("Instances").size("Features")

    val generator = new moa.streams.generators.RandomRBFGenerator()
    generator.numClassesOption.setValue(numClasses)
    generator.numCentroidsOption.setValue(numCentroids)
    generator.numAttsOption.setValue(featureCount)
    generator.instanceRandomSeedOption.setValue(Random.nextInt)
    generator.prepareForUse()
    generator.restart()

    generator
  }
}