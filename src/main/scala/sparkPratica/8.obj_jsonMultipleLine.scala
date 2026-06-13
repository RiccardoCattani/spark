// Scopo dello script
// ------------------
// Questo script dimostra due modalita' diverse di lettura JSON con Spark.
//
// Il primo caso legge un JSON semplice, dove ogni riga del file rappresenta un
// record JSON autonomo. Questo e' il formato piu' comune per dataset JSON in
// ambito Big Data.
//
// Il secondo caso legge un JSON multiLine, cioe' un documento JSON formattato su
// piu righe e potenzialmente annidato. In questo caso serve l'opzione
// multiLine = true, altrimenti Spark prova a interpretare ogni riga come record
// separato.
//
package sparkPratica

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.explode

// Questo script mostra due modalita' di lettura JSON con Spark:
// 1. JSON semplice: un record JSON per ogni riga del file.
// 2. JSON multiLine: un documento JSON su piu righe, spesso con strutture annidate.
object obj_jsonMultipleLine {
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
      .load("C:\\repository\\spark\\1.input\\user.json")
    showDataFrameDetails("JSON semplice: un record JSON per riga", simpleDf)

    // Lettura JSON multiLine: utile quando il file contiene un unico documento
    // JSON formattato su piu righe.
    val complexDf = spark.read
      .format("json")
      .option("multiLine", true)
      .load("C:\\repository\\spark\\1.input\\random_user.json")
    showDataFrameDetails("JSON complesso multiLine: documento annidato", complexDf)

    // Trasforma l'array results in righe e seleziona i campi annidati come colonne.
    val usersDf = complexDf
      .select(
        col("nationality"),
        col("seed"),
        col("version"),
        explode(col("results")).alias("result")
      )
      .select(
        col("nationality"),
        col("seed"),
        col("version"),
        col("result.user.gender").alias("gender"),
        col("result.user.name.title").alias("title"),
        col("result.user.name.first").alias("first_name"),
        col("result.user.name.last").alias("last_name"),
        col("result.user.location.street").alias("street"),
        col("result.user.location.city").alias("city"),
        col("result.user.location.state").alias("state"),
        col("result.user.location.zip").alias("zip"),
        col("result.user.email").alias("email"),
        col("result.user.username").alias("username"),
        col("result.user.phone").alias("phone")
      )
    showDataFrameDetails("JSON multiLine convertito in righe e colonne", usersDf)

    // Chiude la SparkSession.
    spark.stop()
  }
}
