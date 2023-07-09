package com.fabulouslab.spark.e2_dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object E07_AGG {

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder
      .master("local[2]")
      .appName("exo-3")
      .getOrCreate()

    import sparkSession.implicits._

    val videos = sparkSession.read
      .option("header", "true")
      .csv("src/main/resources/USvideos.csv")

    // Nombre de vidéos par channel
    val videosPerChannel = videos.groupBy($"channel_title")
      .count()
    videosPerChannel.show()

    // Max de views par channel
    val maxViewsPerChannel = videos.groupBy($"channel_title")
      .agg(max($"views").alias("max_views"))
    maxViewsPerChannel.show()

    // Nombre de vidéos par channel et catégorie
    val videosPerChannelAndCategory = videos.groupBy($"channel_title", $"category_id")
      .count()
    videosPerChannelAndCategory.show()

    // Catégorie la plus regardée
    val mostViewedCategory = videos.groupBy($"category_id")
      .agg(sum($"views").alias("total_views"))
      .orderBy($"total_views".desc)
      .limit(1)
    mostViewedCategory.show()

    // Catégorie la moins regardée
    val leastViewedCategory = videos.groupBy($"category_id")
      .agg(sum($"views").alias("total_views"))
      .orderBy($"total_views".asc)
      .limit(1)
    leastViewedCategory.show()

    // Nombre de vidéos qui n'ont pas été regardées et le nombre de vidéos avec plus de 10000 views
    val videosViewStats = videos.select($"views".cast("Int"))
      .agg(sum(when($"views" === 0, 1).otherwise(0)).alias("unviewed_videos"),
        sum(when($"views" > 10000, 1).otherwise(0)).alias("videos_with_more_than_10000_views"))
    videosViewStats.show()

    sparkSession.close()
  }

  def viewToTuple(view: Int) = {
    if (view == 0)
      (1, 0)
    else if (view > 1000)
      (0, 1)
    else
      (0, 0)
  }
}
