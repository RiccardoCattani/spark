package sparkPractise

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

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
    val spark = SparkSession.builder()
      .appName("India DataFrame Example")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read
      .option("header", "false")
      .option("inferSchema", "false")
      .option("delimiter", ",")
      .csv("India.txt")
      .toDF("Stato", "Capitale", "Lingua")
      .cache()

    showDataFrameDetails("Tutti i dati caricati da India.txt", df)

    val hindiDf = df.filter(df("Lingua") === "Hindi")
    showDataFrameDetails("Filtro DataFrame: Lingua = Hindi", hindiDf)

    printSection("Riepilogo per lingua")
    df.groupBy("Lingua")
      .count()
      .orderBy("Lingua")
      .show(MaxRowsToShow, truncate = false)

    spark.stop()
  }
}
