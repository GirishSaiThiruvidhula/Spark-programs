import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object GetTopFiveRanks {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("Window Function RANK").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions","2")

    val orders = spark.read.json("D:\\Hadoop\\DATASETS\\data-master\\retail_db_json\\orders")
    val orderItems = spark.read.json("D:\\Hadoop\\DATASETS\\data-master\\retail_db_json\\order_items")

    val dailyProductRevenue = orders.filter(col("order_status").isin("COMPLETE","CLOSED"))
      .join(orderItems, col("order_id") === col("order_item_order_id"))
      .groupBy("order_date","order_item_product_id")
      .agg(sum("order_item_subtotal").alias("daily_revenue_per_product"))
      .orderBy(col("order_date"),col("daily_revenue_per_product").desc)

    val spec = Window.partitionBy("order_date").orderBy(col("daily_revenue_per_product").desc)
    val avg_rev = avg(col("daily_revenue_per_product")).over(spec)
    val ar = dailyProductRevenue.withColumn("Avg_REV",avg_rev)
    ar.show()

    val rnk = rank().over(Window.partitionBy("order_date").orderBy(col("daily_revenue_per_product").desc))
    val topNDailyProducts = dailyProductRevenue.withColumn("RANK",rnk).filter(col("RANK") <= "5")
        .orderBy(col("order_date"),col("RANK"))
    topNDailyProducts.show()


  }
}
