package sparkly.math.stat

import java.nio.ByteBuffer

import org.apache.mahout.math.stats.TDigest

class FeatureSummary(val count: Long, val missing: Long, val stats: FeatureStatistic) extends Serializable {
  def mean: Double = stats.mean
  def stdev: Double = stats.stdev
  def min: Double = stats.min
  def max: Double = stats.max
  def quantile(q: Double) = stats.quantile(q)

  def merge(other: FeatureSummary): FeatureSummary = {
    new FeatureSummary(this.count + other.count, this.missing + other.missing, this.stats.merge(other.stats))
  }
}

object FeatureSummary {

  def apply(features: Iterator[Double]) : FeatureSummary = {
    val (missing, values) = features.toList.partition(feature => feature.isNaN)
    new FeatureSummary(missing.size + values.size, missing.size, FeatureStatistic(values))
  }

  def apply(feature: Double) : FeatureSummary = {
    val missing = if(feature.isNaN) 1 else 0
    new FeatureSummary(1, missing , FeatureStatistic(feature))
  }

  def zero(): FeatureSummary = {
    new FeatureSummary(0L, 0L, new FeatureStatistic())
  }
}

class FeatureStatistic(values: TraversableOnce[Double]) extends Serializable {
  private var n: Long = 0     // Running count of our values
  private var mu: Double = 0  // Running mean of our values
  private var m2: Double = 0  // Running variance numerator (sum of (x - mean)^2)
  private var maxValue: Double = Double.NegativeInfinity // Running max of our values
  private var minValue: Double = Double.PositiveInfinity // Running min of our values
  private var quantiles: SerializableTDigest = SerializableTDigest(new TDigest(100))

  merge(values)

  def this() = this(Nil)

  def merge(value: Double): FeatureStatistic = {
    val delta = value - mu
    n += 1
    mu += delta / n
    m2 += delta * (value - mu)
    maxValue = math.max(maxValue, value)
    minValue = math.min(minValue, value)
    quantiles = quantiles.add(value)
    this
  }

  def merge(values: TraversableOnce[Double]): FeatureStatistic = {
    values.foreach(v => merge(v))
    this
  }

  def merge(other: FeatureStatistic): FeatureStatistic = {
    if (other == this) {
      merge(other.copy())  // Avoid overwriting fields in a weird order
    } else {
      if (n == 0) {
        mu = other.mu
        m2 = other.m2
        n = other.n
        maxValue = other.maxValue
        minValue = other.minValue
        quantiles = other.quantiles
      } else if (other.n != 0) {
        val delta = other.mu - mu
        if (other.n * 10 < n) {
          mu = mu + (delta * other.n) / (n + other.n)
        } else if (n * 10 < other.n) {
          mu = other.mu - (delta * n) / (n + other.n)
        } else {
          mu = (mu * n + other.mu * other.n) / (n + other.n)
        }
        m2 += other.m2 + (delta * delta * n * other.n) / (n + other.n)
        n += other.n
        maxValue = math.max(maxValue, other.maxValue)
        minValue = math.min(minValue, other.minValue)
        quantiles = quantiles.add(other.quantiles)
      }
      this
    }
  }

  def copy(): FeatureStatistic = {
    val other = new FeatureStatistic
    other.n = n
    other.mu = mu
    other.m2 = m2
    other.maxValue = maxValue
    other.minValue = minValue
    other.quantiles = quantiles
    other
  }

  def count: Long = n

  def mean: Double = mu

  def sum: Double = n * mu

  def max: Double = maxValue

  def min: Double = minValue

  def variance: Double = {
    if (n == 0) {
      Double.NaN
    } else {
      m2 / n
    }
  }

  def sampleVariance: Double = {
    if (n <= 1) {
      Double.NaN
    } else {
      m2 / (n - 1)
    }
  }

  /** Return the standard deviation of the values. */
  def stdev: Double = math.sqrt(variance)

  def sampleStdev: Double = math.sqrt(sampleVariance)

  def quantile(q: Double): Double = {
    val tDigest = quantiles.get
    if(tDigest == null || tDigest.size() <= 1) {null.asInstanceOf[Double]} else {tDigest.quantile(q)}
  }

  override def toString: String = {
    "(count: %d, mean: %f, stdev: %f, max: %f, min: %f)".format(count, mean, stdev, max, min)
  }
}

object FeatureStatistic {
  def apply(values: TraversableOnce[Double]) = new FeatureStatistic(values)
  def apply(values: Double*) = new FeatureStatistic(values)
  def apply() = new FeatureStatistic()
}


class SerializableTDigest(val bytes: Array[Byte]) extends Serializable {

  @transient private var tDigest: TDigest = null

  def get(): TDigest = Option(tDigest) match {
    case Some(tDigest) => tDigest
    case None => {
      val buffer = ByteBuffer.wrap(bytes)
      tDigest = if(bytes.length > 1) {TDigest.fromBytes(buffer)} else {new TDigest(100)}
      tDigest
    }
  }

  def add(value: Double): SerializableTDigest = {
    val tDigest = get()
    tDigest.add(value)
    SerializableTDigest(tDigest)
  }


  def add(other: SerializableTDigest): SerializableTDigest = {
    val tDigest = get()
    tDigest.add(other.get())
    SerializableTDigest(tDigest)
  }
}

object SerializableTDigest {

  def apply(tDigest: TDigest): SerializableTDigest = {
    if (tDigest.size() < 1) {
      // No group or null group that cause NPE
      new SerializableTDigest(Array[Byte](0))
    } else {
      val size = tDigest.byteSize()
      val buffer = ByteBuffer.allocate(size)
      tDigest.asBytes(buffer)
      new SerializableTDigest(buffer.array())
    }
  }
}