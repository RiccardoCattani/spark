package sparkPratica

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object adding_removing_updating_Cols {
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
    val conf = new SparkConf().setAppName("bank_Trans").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")

    val spark = SparkSession.builder().getOrCreate()

    val read_csv_df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("C:\\Users\\Riccardo\\Downloads\\2010-12-01.csv")
      .persist()

    showDataFrameDetails("CSV transazioni bancarie letto con inferSchema", read_csv_df)

    printSection("Statistiche descrittive colonne numeriche/stringa")
    read_csv_df.describe().show(MaxRowsToShow, truncate = false)

    printSection("Conteggio valori null per colonna")
    val nullCounts = read_csv_df.columns.map { columnName =>
      sum(when(col(columnName).isNull || trim(col(columnName).cast("string")) === "", 1).otherwise(0)).alias(columnName)
    }
    read_csv_df.select(nullCounts: _*).show(truncate = false)

    spark.stop()
    sc.stop()
  }
}
