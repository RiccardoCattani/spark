package sparkPratica

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

// Questo script mostra due modalita' di lettura JSON con Spark:
// 1. JSON semplice: un record JSON per ogni riga del file.
// 2. JSON multiLine: un documento JSON su piu righe, spesso con strutture annidate.
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
    // Crea la SparkSession per lavorare con DataFrame.
    val spark = SparkSession.builder()
      .appName("Read JSON Example")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // Lettura JSON standard: Spark si aspetta un oggetto JSON per riga.
    val simpleDf = spark.read
      .format("json")
      .load("/home/riccardo/Documenti/repository/spark/spark/user.json")
    showDataFrameDetails("JSON semplice: un record JSON per riga", simpleDf)

    // Lettura JSON multiLine: utile quando il file contiene un unico documento
    // JSON formattato su piu righe.
    val complexDf = spark.read
      .format("json")
      .option("multiLine", true)
      .load("/home/riccardo/Documenti/repository/spark/spark/random_user.json")
    showDataFrameDetails("JSON complesso multiLine: documento annidato", complexDf)

    // Chiude la SparkSession.
    spark.stop()
  }
}
