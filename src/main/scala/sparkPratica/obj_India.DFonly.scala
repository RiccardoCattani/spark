package sparkPractise

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

// Questo script mostra la versione solo DataFrame dell'esercizio su India.txt.
// A differenza degli esempi RDD, qui i dati vengono letti direttamente come DataFrame
// e poi interrogati tramite colonne, filtri e groupBy.
object obj_IndiaDF2 {
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
    // Crea la SparkSession, punto di ingresso principale per DataFrame e SQL.
    val spark = SparkSession.builder()
      .appName("India DataFrame Example")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // Legge India.txt come CSV senza header.
    // toDF assegna manualmente i nomi alle tre colonne lette dal file.
    val df = spark.read
      .option("header", "false")
      .option("inferSchema", "false")
      .option("delimiter", ",")
      .csv("India.txt")
      .toDF("Stato", "Capitale", "Lingua")
      .cache()

    showDataFrameDetails("Tutti i dati caricati da India.txt", df)

    // Filtra il DataFrame mantenendo solo gli stati con lingua Hindi.
    val hindiDf = df.filter(df("Lingua") === "Hindi")
    showDataFrameDetails("Filtro DataFrame: Lingua = Hindi", hindiDf)

    // Raggruppa per lingua e conta quanti record appartengono a ogni lingua.
    printSection("Riepilogo per lingua")
    df.groupBy("Lingua")
      .count()
      .orderBy("Lingua")
      .show(MaxRowsToShow, truncate = false)

    // Chiude la SparkSession.
    spark.stop()
  }
}
