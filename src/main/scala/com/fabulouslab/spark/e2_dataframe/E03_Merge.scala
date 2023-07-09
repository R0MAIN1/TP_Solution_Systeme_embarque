package com.fabulouslab.spark.e2_dataframe

import org.apache.spark.sql.{SaveMode, SparkSession}

object E03_Merge {

  def main(args: Array[String]) {

    /**
     *  Notre startup est mytube connait une augmentation exponentielle du trafic,
     *  l'equipe marketing, nous as chargé d'étudier le dataset des filliales Us et GB.
     *  Mais avant de commencer l'étude, on doit fusioner les deux datasets:
     *  - Chargez le fichier USvideos.csv
     *  - Faire un show sur ce Dataset pour explorer les données
     *  - C'est quoi son schema ?
     *  - Essayez de forcer Spark à déduire le bon schéma
     *  - Chargez le fichier GBvideos.csv avec avec les mêmes paramètres que le USvideos.csv
     *  - Fussionez les deux datasets dans un fichier videos.parquet au format parquet ?
     *  - À quoi ressemble le fichier videos.parquet ?
     *  - Lancez ce job plusieurs fois en jouant  avec le SaveMode, pour écrasez le fichier à chaque lancement
     *
     * */
    val sparkSession = SparkSession.builder
      .master("local[1]")
      .appName("exo-3")
      .getOrCreate()

    val usVideos = sparkSession.read
      .option("header", "true")
      .csv("src/main/resources/USvideos.csv")

    // Afficher le schema du DataFrame USVideos
    println("Schema du DataFrame USVideos :")
    usVideos.printSchema()

    // Essayer de forcer Spark à déduire le bon schéma
    val usVideosWithSchema = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/USvideos.csv")

    // Afficher le schema du DataFrame USVideos avec le bon schéma déduit
    println("Schema du DataFrame USVideos avec le bon schéma déduit :")
    usVideosWithSchema.printSchema()

    // Charger le fichier GBvideos.csv avec les mêmes paramètres que USvideos.csv
    val gbVideos = sparkSession.read
      .option("header", "true")
      .csv("src/main/resources/GBvideos.csv")

    // Fusionner les deux DataFrames en utilisant l'opération union
    val mergedVideos = usVideos.union(gbVideos)

    // Enregistrer le DataFrame fusionné au format Parquet
    mergedVideos.write
      .mode(SaveMode.Overwrite) // Pour écraser le fichier à chaque lancement
      .parquet("videos.parquet")

    // Lire le fichier Parquet et afficher son contenu
    val parquetFile = sparkSession.read.parquet("videos.parquet")
    println("Contenu du fichier videos.parquet :")
    parquetFile.show()

    sparkSession.stop()
  }
}
