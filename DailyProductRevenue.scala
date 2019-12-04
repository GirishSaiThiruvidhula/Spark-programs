import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DailyProductRevenue {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("DailyProductRevenue").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val orders = spark.read.json("D:\\Hadoop\\DATASETS\\data-master\\retail_db_json\\orders")
    val orderItems = spark.read.json("D:\\Hadoop\\DATASETS\\data-master\\retail_db_json\\order_items")

    val dailyProductRevenue = orders.filter(col("order_status").isin("COMPLETE","CLOSED"))
      .join(orderItems, col("order_id") === col("order_item_order_id"))
      .groupBy("order_date","order_item_product_id")
      .agg(sum("order_item_subtotal").alias("daily_revenue_per_product"))
      .orderBy(col("order_date"),col("daily_revenue_per_product").desc)

    val outBaseDir = "D:\\Hadoop\\Temp"

    dailyProductRevenue.write.json(outBaseDir +"/getDailyProductRevenue")
  }
}
