import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator
object AccumlatorsExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("Accumulators in Spark").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val df = spark.read.json("D:\\Hadoop\\DATASETS\\data-master\\retail_db_json\\categories")

    // Unnamed accumulator
    val kids = new LongAccumulator
    spark.sparkContext.register(kids)

    // Named Accumulator

    val men = new LongAccumulator
    spark.sparkContext.register(men,name = "MenAcc")
    // 2nd way of creating accumulator

    val women = spark.sparkContext.longAccumulator(name = "WomenAcc")
    var count = 0
    df.foreach {
      row=>
        val name = row(2).toString

        if(name.contains("Kids")){
          count = count + 1
          kids.add(count)
        }

        if(name.contains("Men")){
          count = count + 1
          men.add(count)
        }

        if(name.contains("Women")){
          count = count + 1
          women.add(count)
        }
    }
    println(s"---------Kids-----------: ${kids.value}")
    println(s"---------Men------------: $men")
    println(s"---------Women----------: $women")
  }
}
