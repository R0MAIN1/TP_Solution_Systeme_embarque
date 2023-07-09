package com.fabulouslab.spark.e04_stream

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._

object E01_hello_kafka {
  def main(args: Array[String]) {
    // Création de l'instance SparkSession
    val spark = SparkSession.builder
      .appName("E01_hello_kafka")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    // Configuration des options Kafka
    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer].getName,
      "value.deserializer" -> classOf[StringDeserializer].getName,
      "group.id" -> "hello_kafka_group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> "false"
    ).mapValues(_.toString)

    // Spécifiez les topics à consommer
    val topics = Array("pageviews")

    // Lecture en continu à partir de Kafka
    val kafkaStream = spark.readStream
      .format("kafka")
      .options(kafkaParams)
      .option("subscribe", topics.mkString(","))
      .load()

    // Conversion des données en chaînes de caractères
    val values = kafkaStream.selectExpr("CAST(value AS STRING)").as[String]

    // Affichez le contenu du flux dans la console
    val query = values.writeStream
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
