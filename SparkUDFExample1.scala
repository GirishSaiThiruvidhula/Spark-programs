import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object SparkUDFExample1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("SparkUDF Apporach1").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val stocks = spark.read.options(Map("header" -> "true" , "inferSchema" -> "true"))
      .csv("D:\\Hadoop\\DATASETS\\TalentOrigin\\nse-stocks")


    // 2. Convert the function into UDF
    val toLowerUDF = udf[String,String](toLower)

    // Make Spark Dataframes/Dataset to use UDF
    stocks.select(toLowerUDF(stocks("SYMBOL")).alias("LOWER_CASE")).show()

  }
  // 1. Define a function
  def toLower(s:String): String = s.toLowerCase()
}
