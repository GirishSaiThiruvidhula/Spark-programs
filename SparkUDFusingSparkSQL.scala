import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object SparkUDFusingSparkSQL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("SparkUDF Apporach1").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val stocks = spark.read.options(Map("header" -> "true" , "inferSchema" -> "true"))
      .csv("D:\\Hadoop\\DATASETS\\TalentOrigin\\nse-stocks")
    //2. Convert function to UDF
    val averageUDF = udf[Double,Double,Double](average)

    // 3. Register the UDF
    spark.sqlContext.udf.register("average_udf",averageUDF)
    //4. Convert Dataframe to temp table or view
    stocks.createOrReplaceTempView("stocks_table")
    spark.sql("select SYMBOL, HIGH, LOW, average_udf(HIGH,LOW) as AVERAGE from stocks_table").show()

  }

  //1. Define a function
  def average(num1: Double, num2: Double): Double = (num1 + num2)/2.0

}
