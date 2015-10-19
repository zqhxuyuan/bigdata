package sparkly.component.source.generator

import moa.streams.InstanceStream
import sparkly.core.FeatureType._
import sparkly.core.PropertyType.{INTEGER, _}
import sparkly.core._
import scala.util.Random

class AgrawalGenerator extends MoaFixedFeatureGenerator {

  override def metadata = super.metadata.copy (
    name = "Agrawal generator",
    description = "Stream generator for Agrawal dataset. Generator described in paper:\nRakesh Agrawal, Tomasz Imielinksi, and Arun Swami, \"Database Mining: A Performance Perspective\", IEEE Transactions on Knowledge and Data Engineering, 5(6), December 1993. \n\nPublic C source code available at:\nhttp://www.almaden.ibm.com/cs/projects/iis/hdb/Projects/data_mining/datasets/syndata.html",
    properties = super.metadata.properties ++ Map (
      "Classification function used" -> PropertyMetadata(INTEGER, defaultValue = Some(1), acceptedValues = (1 to 10).map(_.toString).toList),
      "Amount of noise introduced to numeric values [0.0 1.0]." -> PropertyMetadata(DECIMAL, defaultValue = Some(0.05))
    )
  )

  def features: List[(String, FeatureType)] = List (
    ("salary", FeatureType.DOUBLE),
    ("commission", FeatureType.DOUBLE),
    ("age", FeatureType.INTEGER),
    ("elevel", FeatureType.STRING),
    ("car", FeatureType.STRING),
    ("zipcode", FeatureType.STRING),
    ("hvalue", FeatureType.DOUBLE),
    ("hyears", FeatureType.INTEGER),
    ("loan", FeatureType.DOUBLE),
    ("class", FeatureType.STRING)
  )

  def generator(context: Context): InstanceStream = {
    val function = context.property("Classification function used").as[Int]
    val noise = context.property("Amount of noise introduced to numeric values [0.0 1.0].").as[Double]

    val generator = new moa.streams.generators.AgrawalGenerator()
    generator.functionOption.setValue(function)
    generator.peturbFractionOption.setValue(noise)
    generator.instanceRandomSeedOption.setValue(Random.nextInt)
    generator.prepareForUse()
    generator.restart()

    generator
  }
}