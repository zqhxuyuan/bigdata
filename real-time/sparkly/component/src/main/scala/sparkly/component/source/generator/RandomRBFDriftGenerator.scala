package sparkly.component.source.generator

import moa.streams.InstanceStream
import sparkly.core.PropertyType.INTEGER
import sparkly.core._
import scala.util.Random

class RandomRBFDriftGenerator extends MoaSelectedFeatureGenerator {

   override def metadata = super.metadata.copy (
     name = "Random RBF with drift generator",
     description = "Generates a random radial basis function stream with drift.",
     properties = super.metadata.properties ++ Map (
       "Number of classes" -> PropertyMetadata(INTEGER, defaultValue = Some(2)),
       "Number of centroids in the model" -> PropertyMetadata(INTEGER, defaultValue = Some(50)),
       "Speed of change of centroids in the model" -> PropertyMetadata(PropertyType.DECIMAL, defaultValue = Some(0.0))
     )
   )

   def generator(context: Context): InstanceStream = {
     val numClasses = context.property( "Number of classes").as[Int]
     val numCentroids = context.property("Number of centroids in the model").as[Int]
     val featureCount = context.outputMappers("Instances").size("Features")
     val speedOfChange = context.property("Speed of change of centroids in the model").as[Double]

     val generator = new moa.streams.generators.RandomRBFGeneratorDrift()
     generator.numClassesOption.setValue(numClasses)
     generator.numCentroidsOption.setValue(numCentroids)
     generator.numAttsOption.setValue(featureCount)
     generator.numDriftCentroidsOption.setValue(numCentroids)
     generator.speedChangeOption.setValue(speedOfChange)
     generator.instanceRandomSeedOption.setValue(Random.nextInt)
     generator.prepareForUse()
     generator.restart()

     generator
   }
 }