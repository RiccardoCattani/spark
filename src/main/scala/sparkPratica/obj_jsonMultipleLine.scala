package sparkPratica

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

object ReadJsonExample {
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
    println("Schema JSON interpretato da Spark:")
    df.printSchema()
    println(s"Dati mostrati: $rowsToShow righe su $totalRows")
    df.show(rowsToShow, truncate = false)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Read JSON Example")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val simpleDf = spark.read
      .format("json")
      .load("/home/riccardo/Documenti/repository/spark/spark/user.json")
    showDataFrameDetails("JSON semplice: un record JSON per riga", simpleDf)

    val complexDf = spark.read
      .format("json")
      .option("multiLine", true)
      .load("/home/riccardo/Documenti/repository/spark/spark/random_user.json")
    showDataFrameDetails("JSON complesso multiLine: documento annidato", complexDf)

    spark.stop()
  }
}
