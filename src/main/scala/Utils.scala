import org.apache.spark.rdd.RDD

object Utils {
  def mean(values: Iterable[Double]): Double = values.sum / values.count(_ => true)

  def variance(items: Iterable[Double]): Double = {
    val mean = Utils.mean(items)
    val sl = items.map(item => Math.pow(item - mean, 2))
    sl.sum / (sl.size - 1)
  }

  // TODO: create util method for resample
}