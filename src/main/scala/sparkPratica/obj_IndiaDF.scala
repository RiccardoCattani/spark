package sparkPractise

import org.apache.spark.sql.SparkSession

object obj_IndiaDF {
  def main(args: Array[String]): Unit = {
    // Crea la SparkSession (necessaria per DataFrame API)
    val spark = SparkSession.builder()
      .appName("India DataFrame Example")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // Legge il file come DataFrame senza header, con delimitatore virgola
    val df = spark.read
      .option("header", "false")
      .option("inferSchema", "false")
      .option("delimiter", ",")
      .csv("India.txt")
      .toDF("Stato", "Capitale", "Lingua")

    // Mostra tutte le righe
    println("--- Tutti i dati ---")
    df.show(false)

    // Filtra e mostra solo gli stati dove la lingua è Hindi
    println("--- Stati con lingua Hindi ---")
    df.filter(df("Lingua") === "Hindi").show(false)

    // Ferma la sessione Spark
    spark.stop()
  }
}
