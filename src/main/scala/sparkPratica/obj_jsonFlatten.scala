package sparkPratica

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object obj_jsonFlatten {
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
      .appName("Flatten JSON Example")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val complexDf = spark.read
      .format("json")
      .option("multiLine", true)
      .load("/home/riccardo/Documenti/repository/spark/spark/random_user.json")
      .cache()
    showDataFrameDetails("FASE 1 - JSON complesso originale", complexDf)

    val flatDf = complexDf.withColumn("result", explode(col("results"))).cache()
    showDataFrameDetails("FASE 2 - Array results esploso con explode", flatDf)

    val selectedDf = flatDf.select(
      col("nationality"),
      col("result.user.gender"),
      col("result.user.email"),
      col("result.user.name.first").alias("first_name"),
      col("result.user.name.last").alias("last_name"),
      col("result.user.location.city").alias("city"),
      col("result.user.location.state").alias("state")
    )
    showDataFrameDetails("FASE 3 - Colonne annidate selezionate e rinominate", selectedDf)

    spark.stop()
  }
}
