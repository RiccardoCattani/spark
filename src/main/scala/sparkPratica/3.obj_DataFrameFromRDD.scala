/*
  Scopo dello script
  ------------------
  Questo script mostra il passaggio da RDD non strutturato a DataFrame con schema.
  Partiamo da un file di testo, dividiamo ogni riga in colonne e usiamo una case class
  per assegnare nomi e tipi ai campi.

  L'obiettivo e' dimostrare che un RDD di stringhe puo' diventare un DataFrame
  strutturato quando ogni riga viene trasformata in un oggetto Scala tipizzato.
  La case class CountryDml definisce lo schema logico: state, capital, language
  e country.
*/

package sparkPractise

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

case class CountryDml(state: String, capital: String, language: String, country: String)

object obj_DataFrameFromRDD {
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

  def main(arg: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Job1").setMaster("local[*]")
    val sc   = new SparkContext(conf)
    sc.setLogLevel("Error")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val inputFile  = sc.textFile("countries.txt")
    val inputSplit = inputFile.map(x => x.split(",", -1).map(_.trim))

    printSection("RDD sorgente countries.txt")
    println(s"Righe totali nel file: ${inputFile.count()}")
    println("Prime righe:")
    inputFile.take(20).zipWithIndex.foreach {
      case (row, index) => println(f"${index + 1}%3d | $row")
    }

    val inputColumns = inputSplit.map(x => CountryDml(x(0), x(1), x(2), x(3)))
    val dataframe_schema = inputColumns.toDF().cache()

    showDataFrameDetails("DataFrame con schema derivato da case class CountryDml", dataframe_schema)

    printSection("Riepilogo per paese e lingua")
    dataframe_schema
      .groupBy("country", "language")
      .count()
      .orderBy("country", "language")
      .show(MaxRowsToShow, truncate = false)

    spark.stop()
    sc.stop()
  }
}
