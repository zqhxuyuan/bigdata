package sparkly.common

trait Serializer[T] extends Serializable {

  def serialize(value: T): Array[Byte]
}
