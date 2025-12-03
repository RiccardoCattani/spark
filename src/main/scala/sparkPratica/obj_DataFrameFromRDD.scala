package sparkPractise

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object obj_DataFrameFromRDD {
  def main(arg: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrameFromRDD")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("Error")
    import spark.implicits._

    // Percorso di input; aggiorna se il file si trova altrove
    val inputRDD = spark.sparkContext.textFile("/home/riccardo/Documenti/spark/countries.txt")

    // Split per colonna e mapping in Row
    val rowRDD = inputRDD.map(_.split(",")).map(arr => Row(arr(0), arr(1), arr(2), arr(3)))

    // Schema esplicito per creare il DataFrame dal Row RDD
    val schema = StructType(Seq(
      StructField("state", StringType, nullable = true),
      StructField("capital", StringType, nullable = true),
      StructField("language", StringType, nullable = true),
      StructField("country", StringType, nullable = true)
    ))

    val df = spark.createDataFrame(rowRDD, schema)
    df.show(false)

    // Filtra le righe dove la lingua contiene "English"
    val englishDf = df.filter($"language".contains("English"))
    englishDf.show(false)

    // Salva i risultati; overwrite evita l'errore di directory esistente
    englishDf.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "false")
      .csv("/home/riccardo/Documenti/spark/output/English_df")

    spark.stop()
  }
}
