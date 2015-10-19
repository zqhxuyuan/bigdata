package sparkly.utils

object NumberUtils {

  def humanReadableByteCount(bytes: Long, si: Boolean = true): (Double, String) = {
    val base: Double = if(si) 1000 else 1024

    if (bytes < base) return (bytes.toDouble, " B")

    val exp = (Math.log(bytes) / Math.log(base)).toInt
    val unit = (if(si) "kMGTPE" else "KMGTPE").charAt(exp - 1) + (if(si) "" else "i")

    val value = bytes / Math.pow(base, exp)

    (value, unit)
  }

  def toGB(bytes: Long): Double = {
    bytes.toDouble / 1e9
  }

  def toGB(bytes: Long, digits: Int): Double = {
    roundDigits(bytes.toDouble / 1e9, digits)
  }

  def roundDigits(value: Double, digits: Int = 2) = BigDecimal(value).setScale(digits, BigDecimal.RoundingMode.HALF_UP).toDouble

}
