import org.apache.spark.sql.SparkSession

object SparkBroadcastVariablesExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("Broadcast Variables").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val users = spark.sparkContext.textFile("D:\\Hadoop\\DATASETS\\TalentOrigin\\songs_dataset\\users.txt")
    val songs = spark.sparkContext.textFile("D:\\Hadoop\\DATASETS\\TalentOrigin\\songs_dataset\\songs.txt")
    val userSongsPlayCount = spark.sparkContext.textFile("D:\\Hadoop\\DATASETS\\TalentOrigin\\songs_dataset\\user_song_playcounts.txt")

    val usersMap = users.zipWithIndex().map( user => (user._1,user._2)).collectAsMap()
    val songsMap = songs.map(song => song.split(" ")).map(songArr => (songArr(0),songArr(1))).collectAsMap()
    val userSongArray = userSongsPlayCount.map(rec => rec.split("\t"))

    val usersBroadcast = spark.sparkContext.broadcast(usersMap)
    val songsBroadcast = spark.sparkContext.broadcast(songsMap)
    val modifiedCounts = userSongArray.map {
      case Array(uid,sid,count) =>
        val user = usersBroadcast.value.getOrElse(uid, 0)
        val song = songsBroadcast.value.getOrElse(sid, 0)

        (user,song,count)
    }

    println(s"======Count=========: ", modifiedCounts.count())
    modifiedCounts.take(50).foreach(println)
  }
}
