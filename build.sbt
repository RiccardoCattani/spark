name := "SparkPractice"

version := "0.1"

scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0"
)

// Opzioni JVM per compatibilità con Java 17
fork := true
Compile / run / javaOptions ++= Seq(
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", // needed by Spark StorageUtils on Java 17
  "--add-opens=java.base/java.lang=ALL-UNNAMED"
)

addCompilerPlugin("org.scalameta" % "semanticdb-scalac" % "4.8.13" cross CrossVersion.full)
scalacOptions += "-Yrangepos"
