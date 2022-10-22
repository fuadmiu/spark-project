import Utils.{mean, variance}
import org.apache.spark.{SparkConf, SparkContext}

object SparkMain extends App {
  val conf = new SparkConf().setAppName("Spark and SparkSql").setMaster("local")
  val sc = new SparkContext(conf)

  sc.setLogLevel("ERROR")

  val filePath = "input/iris.csv"

  val textFile = sc.textFile(filePath)
  val titleRow = textFile.first()
  val data = textFile.filter(row => row != titleRow) // Assuming csv will always have a title row

  val population = data.map(line => line.split(",")).map(x => (x(5), x(1).toDouble))
  //TODO: Display the population

  val meanVariance = population.groupByKey().sortByKey().mapValues(u => (mean(u), variance(u)))
}