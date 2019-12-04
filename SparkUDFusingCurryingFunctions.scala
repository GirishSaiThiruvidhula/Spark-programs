import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object SparkUDFusingCurryingFunctions {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("SparkUDF Apporach1").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val stocks = spark.read.options(Map("header" -> "true" , "inferSchema" -> "true"))
      .csv("D:\\Hadoop\\DATASETS\\TalentOrigin\\nse-stocks")

    stocks.show()

    val diffCloseData = stocks.withColumn("DIFF_IN_CLOSE_DATA", diffStockCalculation()(stocks("CLOSE"),stocks("PREVCLOSE")))
    diffCloseData.show()

  }
  def diffStockCalculation(): UserDefinedFunction = udf((close: Double, prevClose: Double) => {
    close - prevClose
  })

}
