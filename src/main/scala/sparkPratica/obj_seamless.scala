package sparkPratica

import org.apache.spark.sql.SparkSession

object obj_seamless {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Seamless DataFrame Example")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Lettura di un file CSV in un DataFrame
    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load("percorso/del/tuo/file.csv")

    // Mostra lo schema e i primi 5 record
    df.printSchema()
    df.show(5)

    // Scrittura del DataFrame in formato Parquet
    df.write
      .format("parquet")
      .mode("overwrite")
      .save("percorso/output/parquet")

    // Scrittura del DataFrame in formato JSON
    df.write
      .format("json")
      .mode("overwrite")
      .save("percorso/output/json")
  }
}