import org.apache.spark.{SparkConf, SparkContext}

object MultipleSparkContext {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("Multiple SparkCOntext")
    val sc = new SparkContext(sparkConf)
    val sc1 = new SparkContext(sparkConf)

    val intArray = Array(1,2,3,4,5,6,7,8,9,0)
    val rdd = sc.parallelize(intArray)
    val rdd1 = sc1.parallelize(intArray)

    rdd.collect()
    rdd1.collect()

    // we cannot create morethan one SparkContext in single Jvm
  }
}
