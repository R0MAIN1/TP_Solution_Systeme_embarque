package com.fabulouslab.spark.e2_dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object E04_Filtering {

  def main(args: Array[String]) {

    /**
     * En utilisant filter :
     *   - Comptez le nombre de vidéos qui n'ont aucun commentaire
     *   - Comptez le nombre de vidéos qui ont plus que 10000 commentaires
     *   - Il existe plusieurs manières d'écrire un filtre,  essayez de réécrire
     *     le filtre précédent d'une autre façon pour varier le plaisir !
     *   - C'est quoi le différence entre la fonction where et filter ?
     *
     * */


    val sparkSession = SparkSession.builder
      .master("local[1]")
      .appName("exo-4")
      .getOrCreate()

    import sparkSession.implicits._

    val videos = sparkSession.read
      .option("header", "true")
      .csv("src/main/resources/USvideos.csv")

    // Comptez le nombre de vidéos qui n'ont aucun commentaire
    val videosWithNoComments = videos.filter($"comment_total" === 0)
    val countVideosWithNoComments = videosWithNoComments.count()
    println(s"Number of videos with no comments: $countVideosWithNoComments")

    // Comptez le nombre de vidéos qui ont plus que 10000 commentaires
    val videosWithMoreThan10000Comments = videos.filter($"comment_total" > 10000)
    val countVideosWithMoreThan10000Comments = videosWithMoreThan10000Comments.count()
    println(s"Number of videos with more than 10000 comments: $countVideosWithMoreThan10000Comments")

    // Filtre alternatif en utilisant la syntaxe SQL-like
    val videosWithMoreThan10000CommentsSQL = videos.filter("comment_total > 10000")
    val countVideosWithMoreThan10000CommentsSQL = videosWithMoreThan10000CommentsSQL.count()
    println(s"Number of videos with more than 10000 comments (SQL syntax): $countVideosWithMoreThan10000CommentsSQL")

    // Différence entre la fonction where et filter
    val videosWithNoCommentsWhere = videos.where($"comment_total" === 0)
    val countVideosWithNoCommentsWhere = videosWithNoCommentsWhere.count()
    println(s"Number of videos with no comments (using where): $countVideosWithNoCommentsWhere")

    sparkSession.close()
  }
}