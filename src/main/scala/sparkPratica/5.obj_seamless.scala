package sparkPratica

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

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
    val spark = SparkSession.builder()
      .appName("Seamless DataFrame Example")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load("/home/riccardo/Documenti/repository/spark/spark/test.csv")
      .cache()
    showDataFrameDetails("CSV test.csv letto come DataFrame", df)

    printSection("Scrittura Parquet")
    println("Formato: parquet")
    println("Modalita': overwrite")
    println("Destinazione: percorso/output/parquet")
    df.write
      .format("parquet")
      .mode("overwrite")
      .save("percorso/output/parquet")

    printSection("Scrittura JSON")
    println("Formato: json")
    println("Modalita': overwrite")
    println("Destinazione: percorso/output/json")
    df.write
      .format("json")
      .mode("overwrite")
      .save("percorso/output/json")

    val inputRDD = spark.sparkContext.textFile("C:/SparkScala/SparkScalaPractise/src/main/scala/sparkPractise/logs/logs.txt")
    printSection("RDD creato da SparkContext")
    println("Nota: questo RDD viene creato come esempio di accesso a SparkContext dalla SparkSession.")
    println(s"Numero righe nel file RDD: ${inputRDD.count()}")
    inputRDD.take(20).zipWithIndex.foreach {
      case (row, index) => println(f"${index + 1}%3d | $row")
    }

    spark.stop()
  }
}
