import Utils.{mean, variance}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SparkMain extends App {
  val conf = new SparkConf().setAppName("Spark and SparkSql").setMaster("local")
  val sc = new SparkContext(conf)

  val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
  import spark.implicits._

  sc.setLogLevel("ERROR")

  val filePath = "input/iris.csv"

  val textFile = sc.textFile(filePath)
  val titleRow = textFile.first()
  val data = textFile.filter(row => row != titleRow) // Assuming csv will always have a title row

  val population = data.map(line => line.split(",")).map(x => (x(5).replace("\"",""), x(1).toDouble))
  population.toDF("Species", "Sepal.Length").show(truncate = false)

  val meanVariance = population.groupByKey().sortByKey().mapValues(u => (mean(u), variance(u)))
  meanVariance.toDF("Category", "[Mean, Variance]").show(truncate = false)
}