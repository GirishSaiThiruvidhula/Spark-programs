import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object SparkContextExample {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("First Spark Application").setMaster("local")
    val sc = new SparkContext(sparkConf)
    Logger.getRootLogger().setLevel(Level.ERROR)

    val intArray = Array(1,2,3,4,5,6,7,8,9,0)
    val intRDD = sc.parallelize(intArray)

    println("Number of elements are" + intRDD.count())
    intRDD.foreach(println)

    val fileRDD = sc.textFile("D:\\Hadoop\\DATASETS\\data-master\\retail_db\\categories",1)
    println(fileRDD.first())
  }
}
