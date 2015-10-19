package sparkly.math.stat

import org.scalatest._

class PearsonCorrelationSpec extends FlatSpec with Matchers {

  "PearsonCorrelation" should "compute correlation" in {
    // Given
    val x = (1 to 100).map(_.toDouble)
    val y1 = x.map(_ * 3 + 2)
    val y2 = x.map(_ * -4 + 2)
    val y3 = x.map(anything => Math.random())

    // When / Then
    PearsonCorrelation(x, y1).correlation() should be (1.0 +- 0.1)
    PearsonCorrelation(x, y2).correlation() should be (-1.0 +- 0.1)
    PearsonCorrelation(x, y3).correlation() should be (0.0 +- 0.3)
  }

}
