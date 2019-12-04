import org.apache.spark.sql.SparkSession

object DFFileformats {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("Fileformats").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val jsonDF = spark.read.json("D:\\Hadoop\\DATASETS\\data-master\\retail_db_json\\orders")
    val filterJSON = jsonDF.filter("order_status == 'COMPLETE'")
    filterJSON.show()
    filterJSON.count()
  }
}
