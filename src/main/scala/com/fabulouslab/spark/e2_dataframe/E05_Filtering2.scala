package com.fabulouslab.spark.e2_dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object E05_Filtering2 {

  def main(args: Array[String]) {

    /**
     * Maintenant, on va essayer de faire des filtres plus compliqués :
     *   - trouvez la liste des vidéos dans le channel MTV et le titre contient "Hand In Hand"
     *   - trouvez le nombre de vidéos avec un id catergory 28 ou aucun commentaire ?
     * */

    val sparkSession = SparkSession.builder
      .master("local[1]")
      .appName("exo-5")
      .getOrCreate()

    import sparkSession.implicits._

    val videos = sparkSession.read
      .option("header", "true")
      .csv("src/main/resources/USvideos.csv")

    // Trouvez la liste des vidéos dans le channel MTV et le titre contient "Hand In Hand"
    val mtvVideosWithTitleHandInHand = videos.filter($"channel_title" === "MTV" && $"title".contains("Hand In Hand"))
    mtvVideosWithTitleHandInHand.show()

    // Trouvez le nombre de vidéos avec un id catergory 28 ou aucun commentaire
    val videosWithCategoryId28OrNoComments = videos.filter($"category_id" === 28 || $"comment_total" === 0)
    val countVideosWithCategoryId28OrNoComments = videosWithCategoryId28OrNoComments.count()
    println(s"Number of videos with category ID 28 or no comments: $countVideosWithCategoryId28OrNoComments")

    sparkSession.close()
  }
}
