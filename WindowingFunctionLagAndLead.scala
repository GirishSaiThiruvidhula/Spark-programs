import org.apache.spark.sql.SparkSession

object WindowingFunctionLagAndLead {
  case class Stocks(date: String,Ticker : String,open: Double,high: Double,low: Double,close: Double,volume_for_the_day: Int)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("LagAndLead Windowing function").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val data = spark.sparkContext.textFile("D:\\Hadoop\\DATASETS\\Acadgild").map(x => x.split(","))
      .map(x => (x(0),x(1),x(2).toDouble,x(3).toDouble,x(4).toDouble,x(5).toDouble,x(6).toInt))

    //val dataDF = data.toDF(Stocks)

    data.take(10).foreach(println)



  }
}
