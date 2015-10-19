package sparkly.common

import org.json4s._
import org.json4s.jackson.Serialization

import scala.reflect._

class JsonSerializer[T <: AnyRef](implicit mf: Manifest[T]) extends Serializer[T] {

  override def serialize(value: T): Array[Byte] = {
    implicit val formats = DefaultFormats
    Serialization.write(value).getBytes
  }

}


