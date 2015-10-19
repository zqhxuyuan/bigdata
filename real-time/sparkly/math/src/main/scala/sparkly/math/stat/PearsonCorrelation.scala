package sparkly.math.stat

case class PearsonCorrelation (
  n: Long,
  sx: Double,
  sxx: Double,
  sy: Double,
  syy: Double,
  sxy: Double
) {

  def correlation(): Double = ((n * sxy) - (sx * sy))  / (Math.sqrt(n * sxx -  sx * sx) * Math.sqrt(n * syy - sy * sy))

  def +(other: PearsonCorrelation): PearsonCorrelation = {
    val n = this.n + other.n
    val sx = this.sx + other.sx
    val sxx = this.sxx + other.sxx
    val sy = this.sy + other.sy
    val syy = this.syy + other.syy
    val sxy = this.sxy + other.sxy
    new PearsonCorrelation(n, sx, sxx, sy, syy, sxy)
  }

  def -(other: PearsonCorrelation): PearsonCorrelation = {
    val n = this.n - other.n
    val sx = this.sx - other.sx
    val sxx = this.sxx - other.sxx
    val sy = this.sy - other.sy
    val syy = this.syy - other.syy
    val sxy = this.sxy - other.sxy
    new PearsonCorrelation(n, sx, sxx, sy, syy, sxy)
  }

  def +(x: Double, y: Double): PearsonCorrelation = {
    val n = this.n + 1
    val sx = this.sx + x
    val sxx = this.sxx + x * x
    val sy = this.sy + y
    val syy = this.syy + y * y
    val sxy = this.sxy + x * y
    new PearsonCorrelation(n, sx, sxx, sy, syy, sxy)
  }
}

object PearsonCorrelation {
  def apply(): PearsonCorrelation = {
    new PearsonCorrelation(0, 0, 0, 0, 0, 0)
  }

  def apply(x: Double, y: Double): PearsonCorrelation = {
    new PearsonCorrelation(1, x, x * x, y, y * y,  x * y)
  }

  def apply(x: Iterable[Double], y: Iterable[Double]): PearsonCorrelation = {
    PearsonCorrelation((x zip y).iterator)
  }

  def apply(data: Iterator[(Double, Double)]): PearsonCorrelation = {
    data.map(values => PearsonCorrelation(values._1, values._2)).reduce(_ + _)
  }

}
