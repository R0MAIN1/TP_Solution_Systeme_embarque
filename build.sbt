name := "Formation-Spark-Batch-Streaming"

version := "1.0"

scalaVersion := "2.12.10"

val sparkVersion = "3.3.2"

javaOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint","-Xms512M", "-Xmx10048M", "-XX:+CMSClassUnloadingEnabled")

Test / run / fork := true


resolvers ++= Seq(
      "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
      "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/",
      "Second Typesafe repo" at "https://repo.typesafe.com/typesafe/maven-releases/",
      Resolver.sonatypeRepo("public")
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-avro" % sparkVersion,
  "com.holdenkarau"  %% "spark-testing-base" % "2.4.3_0.12.0" % "test"
)