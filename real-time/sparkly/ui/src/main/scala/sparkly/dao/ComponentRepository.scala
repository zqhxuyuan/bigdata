package sparkly.dao

import java.lang.reflect.Modifier

import sparkly.core._
import org.reflections.Reflections
import scala.collection.JavaConversions._

class ComponentRepository(val componentBasePackage: String) {

  private val reflections = new Reflections(componentBasePackage)

  private val metadatas = reflections
    .getSubTypesOf(classOf[Component])
    .filter(clazz => !clazz.isInterface && !Modifier.isAbstract(clazz.getModifiers()))
    .map(clazz => (clazz.getName() , clazz.newInstance.metadata))
    .toMap

  def components(): Map[String, ComponentMetadata] = metadatas
  def component(name: String) = metadatas.get(name)
  def findByClassName(name: String) = metadatas(name)
}
