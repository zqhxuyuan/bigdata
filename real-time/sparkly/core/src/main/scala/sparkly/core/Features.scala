package sparkly.core

import java.util.Date
import breeze.linalg.{DenseVector, Vector}
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import scala.util.Try

case class FeatureList(values: List[Feature[_]] = List()) {
  def asList: List[Feature[_]] = values
  def asRaw: List[_] = values.map(_.get)
  def asRawOr(default: => Any): List[_] = values.map(_.getOrElse(default))
  def containsUndefined() = values.exists(_.isEmpty)

  def asStringList = values.map(_.asString)
  def asStringList(default: => String) = values.map(_.asStringOr(default))

  def asIntList = values.map(_.asInt)
  def asIntList(default: => Int) = values.map(_.asIntOr(default))

  def asLongList = values.map(_.asLong)
  def asLongList(default: => Long) = values.map(_.asLongOr(default))

  def asDateList = values.map(_.asDate)
  def asDateList(default: => Date) = values.map(_.asDateOr(default))

  def asBooleanList = values.map(_.asBoolean)
  def asBooleanList(default: => Boolean) = values.map(_.asBooleanOr(default))

  def asDoubleList = values.map(_.asDouble)
  def asDoubleList(default: => Double) = values.map(_.asDoubleOr(default))

  def asVectorList = values.map(_.asVector)
  def asVectorList(default: => Vector[Double]) = values.map(_.asVectorOr(default))

  def toDenseVector(default: => Double) = {
    val result = new Array[Double](values.size)
    values.zipWithIndex.foreach{case (f, i) => result(i) = f.asDoubleOr(default)}
    DenseVector(result)
  }
}

object Feature {

  def apply(value: Option[Any]): Feature[_] = value match {
    case Some(s: String) => new StringFeature(Some(s))
    case Some(d: Double) => new DoubleFeature(Some(d))
    case Some(i: Int) => new IntFeature(Some(i))
    case Some(l: Long) => new LongFeature(Some(l))
    case Some(d: Date) => new DateFeature(Some(d))
    case Some(b: Boolean) => new BooleanFeature(Some(b))
    case Some(v: Vector[Double]) => new VectorFeature(Some(v))
    case Some(null) => new EmptyFeature()
    case None => new EmptyFeature()
    case _ => ???
  }

  def apply(value: Any): Feature[_] = if(value != null) {
    value match {
      case v: String => new StringFeature(Option(v))
      case v: Double => new DoubleFeature(Option(v))
      case v: Int => new IntFeature(Option(v))
      case v: Long => new LongFeature(Option(v))
      case v: Date => new DateFeature(Option(v))
      case v: Boolean => new BooleanFeature(Option(v))
      case v: Vector[Double] => new VectorFeature(Option(v))
      case _ => throw new IllegalArgumentException(s"Unsupported feature type ${value.getClass}")
    }
  } else {
    new EmptyFeature()
  }

  def apply() = empty
  def empty = new EmptyFeature()
}

abstract class Feature[T](val value: Option[T]) extends Serializable {

  def isDefined: Boolean = value.isDefined
  def isEmpty: Boolean = value.isEmpty

  def get: T = value.get
  def getOrElse(default: => Any) = value.getOrElse(default)

  def asString: String
  def asDouble: Double
  def asInt: Int
  def asLong: Long
  def asDate: Date
  def asBoolean: Boolean
  def asVector: Vector[Double]

  def asStringOr(default: => String): String = if (isEmpty) default else Try(asString).getOrElse(default)
  def asDoubleOr(default: => Double): Double = if (isEmpty) default else Try(asDouble).getOrElse(default)
  def asIntOr(default: => Int): Int = if (isEmpty) default else Try(asInt).getOrElse(default)
  def asLongOr(default: => Long): Long = if (isEmpty) default else Try(asLong).getOrElse(default)
  def asDateOr(default: => Date): Date = if (isEmpty) default else Try(asDate).getOrElse(default)
  def asBooleanOr(default: => Boolean): Boolean = if (isEmpty) default else Try(asBoolean).getOrElse(default)
  def asVectorOr(default: => Vector[Double]): Vector[Double] = if (isEmpty) default else Try(asVector).getOrElse(default)


  override def equals(o: Any) = o match {
    case other: Feature[T] => this.value == other.value
    case _ => false
  }

  override def hashCode = this.value.hashCode
  override def toString = this.value.toString

}

class StringFeature(value: Option[String]) extends Feature[String](value) {

  def asString: String = value.get
  def asDouble: Double = value.get.toDouble
  def asInt: Int = value.get.toInt
  def asLong: Long = value.get.toLong
  def asDate: Date = DateTime.parse(value.get, ISODateTimeFormat.dateTimeParser.withZoneUTC()).toDate
  def asBoolean: Boolean = value.get match {
    case "true" | "1" => true
    case "false" | "0" | "-1" => false
  }
  def asVector: Vector[Double] = throw new IllegalArgumentException("Unsupported feature conversion")

}


class DoubleFeature(value: Option[Double]) extends Feature[Double](value) {

  def asString: String = value.get.toString
  def asDouble: Double = value.get
  def asInt: Int = value.get.toInt
  def asLong: Long = value.get.toLong
  def asDate: Date = throw new IllegalArgumentException("Unsupported feature conversion")
  def asBoolean: Boolean = value.get > 0.0
  def asVector: Vector[Double] = throw new IllegalArgumentException("Unsupported feature conversion")

}

class IntFeature(value: Option[Int]) extends Feature[Int](value) {

  def asString: String = value.get.toString
  def asDouble: Double = value.get.toDouble
  def asInt: Int = value.get
  def asLong: Long = value.get.toLong
  def asDate: Date = new Date(value.get.toLong)
  def asBoolean: Boolean = value.get > 0
  def asVector: Vector[Double] = throw new IllegalArgumentException("Unsupported feature conversion")

}

class LongFeature(value: Option[Long]) extends Feature[Long](value) {

  def asString: String = value.get.toString
  def asDouble: Double = value.get.toDouble
  def asInt: Int = value.get.toInt
  def asLong: Long = value.get
  def asDate: Date = new Date(value.get)
  def asBoolean: Boolean = value.get > 0L
  def asVector: Vector[Double] = throw new IllegalArgumentException("Unsupported feature conversion")

}

class DateFeature(value: Option[Date]) extends Feature[Date](value) {

  def asString: String = new DateTime(value.get).toString(ISODateTimeFormat.dateTime.withZoneUTC())
  def asDouble: Double = throw new IllegalArgumentException("Unsupported feature conversion")
  def asInt: Int = value.get.getTime.toInt
  def asLong: Long = value.get.getTime
  def asDate: Date = value.get
  def asBoolean: Boolean = throw new IllegalArgumentException("Unsupported feature conversion")
  def asVector: Vector[Double] = throw new IllegalArgumentException("Unsupported feature conversion")

}

class BooleanFeature(value: Option[Boolean]) extends Feature[Boolean](value) {

  def asString: String = value.get.toString
  def asDouble: Double = if(value.get) 1.0 else 0.0
  def asInt: Int = if(value.get) 1 else 0
  def asLong: Long = if(value.get) 1L else 0L
  def asDate: Date = throw new IllegalArgumentException("Unsupported feature conversion")
  def asBoolean: Boolean = value.get
  def asVector: Vector[Double] = throw new IllegalArgumentException("Unsupported feature conversion")

}

class VectorFeature(value: Option[Vector[Double]]) extends Feature[Vector[Double]](value) {

  def asString: String = throw new IllegalArgumentException("Unsupported feature conversion")
  def asDouble: Double = throw new IllegalArgumentException("Unsupported feature conversion")
  def asInt: Int = throw new IllegalArgumentException("Unsupported feature conversion")
  def asLong: Long = throw new IllegalArgumentException("Unsupported feature conversion")
  def asDate: Date = throw new IllegalArgumentException("Unsupported feature conversion")
  def asBoolean: Boolean = throw new IllegalArgumentException("Unsupported feature conversion")
  def asVector: Vector[Double] = value.get
}

class EmptyFeature extends Feature[Any](None) {

  def asString: String = null.asInstanceOf[String]
  def asDouble: Double = null.asInstanceOf[Double]
  def asInt: Int = null.asInstanceOf[Int]
  def asLong: Long = null.asInstanceOf[Long]
  def asDate: Date = null.asInstanceOf[Date]
  def asBoolean: Boolean = null.asInstanceOf[Boolean]
  def asVector: Vector[Double] = null.asInstanceOf[Vector[Double]]

}
