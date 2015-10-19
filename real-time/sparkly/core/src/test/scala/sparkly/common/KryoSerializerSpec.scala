package sparkly.common

import org.scalatest.{Matchers, FlatSpec}
import org.joda.time.DateTime
import java.util.Date

class KryoSerializerSpec extends FlatSpec with Matchers {

  "Kryo serializer" should "serialize Map" in {
    // Given
    val serializer = new KryoSerializer[Map[String, Any]]()
    val expectedData = Map("name" -> "juju", "age" -> 33)

    // When
    val serializedData = serializer.serialize(expectedData)
    val actualData = serializer.deserialize(serializedData)

    // Then
    actualData("name") should be ("juju")
    actualData("age") should be (33)
  }

  "Kryo serializer" should "serialize case class" in {
    // Given
    val serializer = new KryoSerializer[User]()
    val expectedData = User("juju", 33, DateTime.parse("1982-11-17").toDate)

    // When
    val serializedData = serializer.serialize(expectedData)
    val actualData = serializer.deserialize(serializedData)

    // Then
    actualData.name should be ("juju")
    actualData.age should be (33)
    actualData.birth should be (DateTime.parse("1982-11-17").toDate)
  }

  case class User(name: String, age: Int, birth: Date)
}
