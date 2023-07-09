package com.fabulouslab.spark.e2_dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object E06_Sort {

  def main(args: Array[String]) {

    /**
     * En utilisant la fonction sort, retrouvez:
     *   - la liste des  5 vidéos  les plus consultées, avec le nombre de vu
     *   - la liste des  5 vidéos  les moin consultées, avec le nombre de vu
     *   - La video qui a le plus grand nombre de like
     *   - La video qui a le plus grand nombre de dislike
     * */

    val sparkSession = SparkSession.builder
      .master("local[1]")
      .appName("exo-3")
      .getOrCreate()

    import sparkSession.implicits._

    val videos = sparkSession.read
      .option("header", "true")
      .csv("src/main/resources/USvideos.csv")

    // La liste des 5 vidéos les plus consultées, avec le nombre de vues
    val top5MostViewedVideos = videos.select($"title", $"views".cast("Int"))
      .orderBy($"views".desc)
      .limit(5)
    top5MostViewedVideos.show()

    // La liste des 5 vidéos les moins consultées, avec le nombre de vues
    val top5LeastViewedVideos = videos.select($"title", $"views".cast("Int"))
      .orderBy($"views".asc)
      .limit(5)
    top5LeastViewedVideos.show()

    // La vidéo avec le plus grand nombre de likes
    val videoWithMaxLikes = videos.select($"title", $"likes".cast("Int"))
      .orderBy($"likes".desc)
      .limit(1)
    videoWithMaxLikes.show()

    // La vidéo avec le plus grand nombre de dislikes
    val videoWithMaxDislikes = videos.select($"title", $"dislikes".cast("Int"))
      .orderBy($"dislikes".desc)
      .limit(1)
    videoWithMaxDislikes.show()

    sparkSession.close()
  }
}
