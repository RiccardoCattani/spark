name := "SparkPractice"

version := "0.1"

scalaVersion := "2.12.18"

val hadoopHome = "C:\\hadoop\\hadoop-3.3.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  "org.apache.spark" %% "spark-avro" % "3.5.0",
  "com.databricks" %% "spark-xml" % "0.17.0"
)

// Opzioni JVM per compatibilità con Java 17
fork := true
Compile / run / javaOptions ++= Seq(
  s"-Dhadoop.home.dir=$hadoopHome",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", // needed by Spark StorageUtils on Java 17
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED", // needed by Kryo serializer on Java 17
  "--add-opens=java.base/java.io=ALL-UNNAMED", // needed by spark-xml on Java 21
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED", // needed by spark-xml on Java 21
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED", // needed for SerializedLambda on Java 17
  "--add-opens=java.base/java.util=ALL-UNNAMED" // needed for Scala collections serializers on Java 17
)

Compile / run / envVars ++= Map(
  "HADOOP_HOME" -> hadoopHome,
  "PATH" -> s"$hadoopHome\\bin;${sys.env.getOrElse("PATH", "")}"
)

addCompilerPlugin("org.scalameta" % "semanticdb-scalac" % "4.8.13" cross CrossVersion.full)
scalacOptions += "-Yrangepos"
