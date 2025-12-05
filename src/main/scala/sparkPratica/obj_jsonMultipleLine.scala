package sparkPratica

import org.apache.spark.sql.SparkSession

object ReadJsonExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Read JSON Example")
      .master("local[*]")
      .getOrCreate()

    // Lettura JSON semplice
    println("Leggo il file JSON semplice:")
    val simpleDf = spark.read.format("json").load("/home/riccardo/Documenti/repository/spark/sparkuser.json")
    simpleDf.printSchema()
    simpleDf.show(false)

    // Lettura JSON complesso
    println("Leggo il file JSON complesso:")
    val complexDf = spark.read
      .format("json")
      .option("multiLine", true)
      .load("/home/riccardo/Documenti/repository/spark/sparkrandom_user.json")
    complexDf.printSchema()
    complexDf.show(false)

    spark.stop()
  }
}