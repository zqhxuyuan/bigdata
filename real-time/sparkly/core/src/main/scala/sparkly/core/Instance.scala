package sparkly.core

import sparkly.utils.UUIDGen

object Instance {
  def apply(features: (String, _)*): Instance = new Instance(features.toMap)
}

case class Instance(rawFeatures: Map[String, _] = Map(), inputMapper: Option[Mapper] = None, outputMapper: Option[Mapper] = None, uuid: String = UUIDGen.getTimeUUID.toString) {

  def inputFeature(name: String): Feature[_] = {
    val realName = inputMapper.get.featureName(name)
    Feature(rawFeatures.get(realName))
  }

  def inputFeature(name: String, value: Any): Instance = {
    val realName = inputMapper.get.featureName(name)
    this.copy(rawFeatures = rawFeatures + (realName -> value))
  }

  def inputFeatures(name: String): FeatureList = {
    val realNames = inputMapper.get.featuresNames(name)
    val values = realNames.map(realName => rawFeatures.get(realName))
    FeatureList(values.map(value => Feature(value)))
  }

  def inputFeatures(name: String, values: Iterable[_]): Instance = {
    val realNames = inputMapper.get.featuresNames(name)
    val newFeatures = (realNames zip values).toMap
    this.copy(rawFeatures = rawFeatures ++ newFeatures)
  }

  def outputFeature(name: String): Feature[_] = {
    val realName = outputMapper.get.featureName(name)
    Feature(rawFeatures.get(realName))
  }

  def outputFeature(name: String, value: Any): Instance = {
    val realName = outputMapper.get.featureName(name)
    this.copy(rawFeatures = rawFeatures + (realName -> value))
  }

  def outputFeatures(features: (String, _)*): Instance = {
    val realNames = features.map(_._1).map(outputMapper.get.featureName(_))
    val newFeatures = (realNames zip features.map(_._2)).toMap
    this.copy(rawFeatures = rawFeatures ++ newFeatures)
  }

  def outputFeatures(name: String, values: Iterable[_]): Instance = {
    val realNames = outputMapper.get.featuresNames(name)
    val newFeatures = (realNames zip values).toMap
    this.copy(rawFeatures =rawFeatures ++ newFeatures)
  }

  def outputFeatures[T](name: String): FeatureList = {
    val realNames = outputMapper.get.featuresNames(name)
    val values = realNames.map(realName => rawFeatures.get(realName))
    FeatureList(values.map(value => Feature(value)))
  }

  def rawFeature(name: String): Feature[_] = {
    Feature(rawFeatures.get(name))
  }

}