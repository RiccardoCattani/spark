// Scopo dello script
// ------------------
// Questo script dimostra come trasformare un JSON annidato in un DataFrame piu'
// semplice e tabellare.
//
// Il file random_user.json contiene una struttura complessa con un array results
// e campi interni come user.name, user.email e user.location. Spark riesce a
// leggere questa struttura, ma per analizzarla comodamente conviene appiattirla.
//
// Lo script usa explode per trasformare ogni elemento dell'array results in una
// riga separata, poi usa select e alias per estrarre e rinominare i campi
// annidati piu' utili.
//
package sparkPratica

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Questo script mostra come appiattire un JSON annidato.
// Il file random_user.json contiene un array results: explode trasforma ogni
// elemento dell'array in una riga, poi select estrae i campi annidati utili.
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
    printSection("AVVIO - Flatten di JSON annidato")
    println("Obiettivo: leggere random_user.json, esplodere results e selezionare campi annidati.")
    println("Input: C:\\repository\\spark\\1.input\\random_user.json")

    // Crea la SparkSession per lavorare con DataFrame.
    val spark = SparkSession.builder()
      .appName("Flatten JSON Example")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // Legge un JSON multiLine, cioe' un documento JSON distribuito su piu righe.
    printSection("1 - Lettura JSON multiLine originale")
    println("Spark legge il file come un unico documento JSON con multiLine=true.")
    println("Il risultato contiene colonne annidate, per esempio results.")
    val complexDf = spark.read
      .format("json")
      .option("multiLine", true)
      .load("C:\\repository\\spark\\1.input\\random_user.json")
      .cache()
    showDataFrameDetails("FASE 1 - JSON complesso originale", complexDf)

    // explode trasforma ogni elemento dell'array results in una riga separata.
    printSection("2 - Explode dell'array results")
    println("explode(results) crea una riga per ogni elemento dell'array results.")
    println("La nuova colonna result contiene il singolo elemento esploso.")
    val flatDf = complexDf.withColumn("result", explode(col("results"))).cache()
    showDataFrameDetails("FASE 2 - Array results esploso con explode", flatDf)

    // Seleziona campi annidati dentro result.user e li rinomina con alias leggibili.
    printSection("3 - Selezione e rinomina delle colonne annidate")
    println("Selezioniamo campi dentro result.user e li rinominiamo con alias leggibili.")
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

    // Chiude la SparkSession.
    printSection("FINE - Job completato")
    println("Il JSON annidato e' stato trasformato in un DataFrame tabellare.")
    spark.stop()
  }
}
