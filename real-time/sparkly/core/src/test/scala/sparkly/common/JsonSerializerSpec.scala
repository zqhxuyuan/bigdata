package sparkly.common

import org.scalatest.{Matchers, FlatSpec}
import org.joda.time.DateTime
import java.util.Date

class JsonSerializerSpec extends FlatSpec with Matchers {

  "Json serializer" should "serialize Map" in {
    // Given
    val serializer = new JsonSerializer[Map[String, Any]]()
    val expectedData = Map("name" -> "juju", "age" -> 33)

    // When
    val serializedData = serializer.serialize(expectedData)

    // Then
    new String(serializedData) should be ("""{"name":"juju","age":33}""")
  }

  "Json serializer" should "serialize case class" in {
    // Given
    val serializer = new JsonSerializer[User]()
    val expectedData = User("juju", 33, DateTime.parse("1982-11-17").toDate)

    // When
    val serializedData = serializer.serialize(expectedData)

    // Then
    new String(serializedData) should be ("""{"name":"juju","age":33,"birth":"1982-11-17T00:00:00Z"}""")
  }

  case class User(name: String, age: Int, birth: Date)
}
