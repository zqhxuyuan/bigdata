package sparkly.web.resource

import org.json4s.JsonAST.JDouble

import org.scalatest._

class MonitoringDataQueueSpec extends FlatSpec with Matchers {

  "MonitoringDataQueue" should "be bounded" in {
    // Given
    val data = new MonitoringDataQueue()

    // When
    (0 to 500).foreach(i => data.add(JDouble(i)))

    // Then
    val expectedData = (401 to 500).map(i => JDouble(i))
    data.all() should contain theSameElementsInOrderAs expectedData
  }
}
