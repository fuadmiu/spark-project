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
  meanVariance.toDF("Category", "{Mean, Variance}").show(truncate = false)

  val noOfrecords = data.count().toInt/4 // get 25% of the records in the sample
  val setosaSample = population.filter(item => item._1.equals("setosa")).takeSample(false, noOfrecords)
  val versicolorSample = population.filter(item => item._1.equals("versicolor")).takeSample(false, noOfrecords)
  val virginicaSample = population.filter(item => item._1.equals("virginica")).takeSample(false, noOfrecords)

  // Resample 1000 times
  var setosaMeanTotal=0.0
  var setosavarianceTotal=0.0

  var versiColorMeanTotal = 0.0
  var versiColorVarianceTotal = 0.0

  var virginicaMeanTotal = 0.0
  var virginicaVarianceTotal = 0.0
  for (_ <- 1 until 10) {
    // TODO: Resample
        // val setosaSampleRDD=sc.parallelize(setosaSample)
          val setosaReSampleRDD = sc.parallelize(sc.parallelize(setosaSample).takeSample(true,noOfrecords))
            val setosaMeanvariance=setosaReSampleRDD.groupByKey().sortByKey().mapValues(u => (mean(u), variance(u)))
          setosaMeanvariance.toDF("category","Mean , Variance").show(false)
    setosaMeanTotal=setosaMeanTotal + (setosaMeanvariance.first()._2._1);
    setosavarianceTotal=setosavarianceTotal+(setosaMeanvariance.first()._2._2);
    // TODO: Mean and variance
    // TODO: Add to Running Sum


    // TODO: Resample
    // val setosaSampleRDD=sc.parallelize(setosaSample)
    val versiColorReSampleRDD = sc.parallelize(sc.parallelize(versicolorSample).takeSample(true, noOfrecords))
    val  versiColorMeanvariance = versiColorReSampleRDD.groupByKey().sortByKey().mapValues(u => (mean(u), variance(u)))
    versiColorMeanvariance.toDF("category", "Mean , Variance").show(false)
    versiColorMeanTotal = versiColorMeanTotal + (versiColorMeanvariance.first()._2._1);
    versiColorVarianceTotal =  versiColorVarianceTotal + (versiColorMeanvariance.first()._2._2);
    // TODO: Mean and variance
    // TODO: Add to Running Sum


    // TODO: Resample
    // val setosaSampleRDD=sc.parallelize(setosaSample)
    val virginicaReSampleRDD = sc.parallelize(sc.parallelize(virginicaSample).takeSample(true, noOfrecords))
    val virginicaMeanvariance = virginicaReSampleRDD.groupByKey().sortByKey().mapValues(u => (mean(u), variance(u)))
    virginicaMeanvariance.toDF("category", "Mean , Variance").show(false)
    virginicaMeanTotal = virginicaMeanTotal + (virginicaMeanvariance.first()._2._1);
    virginicaVarianceTotal = virginicaVarianceTotal + (virginicaMeanvariance.first()._2._2);
    // TODO: Mean and variance
    // TODO: Add to Running Sum
  }

  println("Overall mean for Setosa" + setosaMeanTotal / 10)
  println("Overall variance for Setosa" + setosavarianceTotal / 10)

  println("Overall mean for Versicolor" + versiColorMeanTotal / 10)
  println("Overall variance for Versicolor" + versiColorVarianceTotal / 10)

  println("Overall mean for Virginica" + virginicaMeanTotal / 10)
  println("Overall variance for Virginica" + virginicaVarianceTotal / 10)
}