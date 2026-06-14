// Scopo dello script
// ------------------
// Questo script dimostra come Spark possa leggere un file CSV come DataFrame e
// salvare lo stesso contenuto in formati diversi, in particolare Parquet e JSON.
//
// L'obiettivo e' mostrare che il DataFrame e' una rappresentazione comune dei
// dati: una volta letto il CSV, Spark puo' scrivere lo stesso dataset in formati
// fisici differenti senza cambiare il modello logico.
//
// Lo script mostra anche che dalla SparkSession si puo' accedere allo
// SparkContext, quindi alle API RDD, tramite spark.sparkContext.
//
// Esempio output atteso
// ---------------------
// Input CSV:
// col1,col2
// A,10
//
// Dopo lettura:
// DataFrame con colonne col1 e col2.
//
// Dopo scrittura:
// percorso/output/parquet/part-*.parquet
// percorso/output/json/part-*.json
//
package sparkPratica

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

// Questo script mostra un esempio semplice di lettura CSV con DataFrame e
// scrittura dello stesso contenuto in Parquet e JSON.
// In fondo crea anche un RDD tramite spark.sparkContext per mostrare che dalla
// SparkSession si puo' accedere anche alle API RDD.
object obj_seamless {
  private val MaxRowsToShow = 100

  private def printSection(title: String): Unit = {
    println()
    println("=" * 90)
    println(title)
    println("=" * 90)
  }

  private def showDataFrameDetails(title: String, df: DataFrame): Unit = {
    printSection(title)
    val totalRows = df.count()
    val rowsToShow = math.min(totalRows, MaxRowsToShow).toInt
    println(s"Numero colonne: ${df.columns.length}")
    println(s"Colonne: ${df.columns.mkString(", ")}")
    println(s"Numero righe: $totalRows")
    println("Schema:")
    df.printSchema()
    println(s"Dati mostrati: $rowsToShow righe su $totalRows")
    df.show(rowsToShow, truncate = false)
  }

  def main(args: Array[String]): Unit = {
    // Crea la SparkSession per lavorare con DataFrame.
    val spark = SparkSession.builder()
      .appName("Seamless DataFrame Example")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // Legge test.csv come DataFrame CSV con header.
    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load("/home/riccardo/Documenti/repository/spark/spark/test.csv")
      .cache()
    showDataFrameDetails("CSV test.csv letto come DataFrame", df)

    // Scrive il DataFrame in formato Parquet.
    // Parquet e' colonnare ed e' adatto ad analisi e query.
    //
    // Prima: DataFrame in memoria.
    // Dopo: directory percorso/output/parquet con file part-*.parquet.
    printSection("Scrittura Parquet")
    println("Formato: parquet")
    println("Modalita': overwrite")
    println("Destinazione: percorso/output/parquet")
    df.write
      .format("parquet")
      .mode("overwrite")
      .save("percorso/output/parquet")

    // Scrive lo stesso DataFrame in formato JSON.
    // JSON e' testuale e semi-strutturato.
    //
    // Prima: DataFrame in memoria.
    // Dopo: directory percorso/output/json con file part-*.json.
    printSection("Scrittura JSON")
    println("Formato: json")
    println("Modalita': overwrite")
    println("Destinazione: percorso/output/json")
    df.write
      .format("json")
      .mode("overwrite")
      .save("percorso/output/json")

    // Esempio di creazione RDD usando lo SparkContext accessibile dalla SparkSession.
    val inputRDD = spark.sparkContext.textFile("C:/SparkScala/SparkScalaPractise/src/main/scala/sparkPractise/logs/logs.txt")
    printSection("RDD creato da SparkContext")
    println("Nota: questo RDD viene creato come esempio di accesso a SparkContext dalla SparkSession.")
    println(s"Numero righe nel file RDD: ${inputRDD.count()}")
    inputRDD.take(20).zipWithIndex.foreach {
      case (row, index) => println(f"${index + 1}%3d | $row")
    }

    // Chiude la SparkSession.
    spark.stop()
  }
}
