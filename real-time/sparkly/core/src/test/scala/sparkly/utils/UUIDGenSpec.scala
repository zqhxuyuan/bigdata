package sparkly.utils

import org.scalatest._

class UUIDGenSpec extends FlatSpec with Matchers {

  "UUIDGen" should "generate distinct uuid" in {
    // Given
    val size = 100000

    // When
    val uuids = (1 to size).map(i => UUIDGen.getTimeUUID.toString)

    // Then
    uuids.toSet should have size (size)
  }
}
