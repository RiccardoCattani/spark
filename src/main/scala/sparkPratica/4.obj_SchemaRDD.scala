// Esecuzione:
// sbt "runMain sparkPractise.obj_SchemaRDD"
//
// Esempio didattico: creazione di DataFrame con schema partendo da un file di testo.
// Il file di input dovrebbe avere righe nel formato:
// state,capital,language,country
//
// Il programma mostra due approcci:
// - case class + toDF(): schema derivato automaticamente dai campi della case class
// - Row + StructType: schema definito manualmente
//
// L'output e' volutamente dettagliato: per ogni passaggio stampa titolo, colonne,
// numero righe, schema e dati mostrati, cosi' non compare solo una tabella anonima.

package sparkPractise

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, Path => NioPath}
import java.util.Comparator
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

case class FileDml(state: String, capital: String, language: String, country: String)

object obj_SchemaRDD {
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

    if (totalRows > MaxRowsToShow) {
      println(s"Nota: il DataFrame contiene $totalRows righe; ne mostro solo $MaxRowsToShow per non riempire troppo la console.")
    }
  }

  private def showLanguageSummary(title: String, df: DataFrame): Unit = {
    printSection(title)

    println("Distribuzione righe per lingua:")
    df.groupBy("language")
      .count()
      .orderBy("language")
      .show(MaxRowsToShow, truncate = false)
  }

  private def saveCsvIfPossible(sc: SparkContext, df: DataFrame, outputPath: String): Unit = {
    printSection("Scrittura output CSV filtrato")

    val isWindows = System.getProperty("os.name").toLowerCase.contains("windows")

    if (isWindows) {
      saveCsvLocallyOnWindows(df, outputPath)
      return
    }

    try {
      val fs   = FileSystem.get(sc.hadoopConfiguration)
      val path = new Path(outputPath)

      if (fs.exists(path)) {
        fs.delete(path, true)
        println(s"Output precedente eliminato: $outputPath")
      }

      df.coalesce(1).write.mode("overwrite").csv(outputPath)
      println(s"Output salvato correttamente in: $outputPath")
    } catch {
      case NonFatal(error) =>
        println("Scrittura CSV non completata.")
        println(s"Percorso richiesto: $outputPath")
        println(s"Errore: ${error.getClass.getSimpleName} - ${error.getMessage}")
        println("Il programma continua comunque per mostrare il resto degli esempi DataFrame.")
    }
  }

  private def saveCsvLocallyOnWindows(df: DataFrame, outputPath: String): Unit = {
    println("Ambiente Windows rilevato: uso una scrittura CSV locale per evitare il problema Hadoop NativeIO.")
    println("Nota: questa fallback usa collect(), quindi e' adatta solo a dataset piccoli di esercizio.")

    val outputDir = Paths.get(outputPath)
    deleteLocalIfExists(outputDir)
    Files.createDirectories(outputDir)

    val header = df.columns.map(csvEscape).mkString(",")
    val rows = df.collect().map { row =>
      row.toSeq
        .map(value => csvEscape(Option(value).map(_.toString).getOrElse("")))
        .mkString(",")
    }

    val outputFile = outputDir.resolve("part-00000-local.csv")
    Files.write(outputFile, (header +: rows).toSeq.asJava, StandardCharsets.UTF_8)
    println(s"Output salvato correttamente in: $outputFile")
  }

  private def csvEscape(value: String): String = {
    val mustQuote = value.exists(ch => ch == ',' || ch == '"' || ch == '\n' || ch == '\r')
    val escaped = value.replace("\"", "\"\"")
    if (mustQuote) "\"" + escaped + "\"" else escaped
  }

  private def deleteLocalIfExists(path: NioPath): Unit = {
    if (Files.exists(path)) {
      Files
        .walk(path)
        .sorted(Comparator.reverseOrder())
        .forEach(p => Files.delete(p))
    }
  }

  def main(arg: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestLog").setMaster("local[*]")
    val sc   = new SparkContext(conf)
    sc.setLogLevel("Error")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    // textFile legge il file come RDD[String]. Ogni elemento e' una riga.
    val inputFile = sc.textFile("C:\\repository\\spark\\1.input\\India.txt")

    // split(",", -1) conserva anche eventuali campi vuoti finali.
    // trim rimuove spazi indesiderati prima e dopo ogni campo.
    val inputSplit = inputFile.map(line => line.split(",", -1).map(_.trim))

    // Trasformiamo ogni riga in una case class. Se la quarta colonna non esiste
    // o e' vuota, usiamo "N/A" come valore di default per country.
    val inputColumns = inputSplit.map { cols =>
      val countryValue = if (cols.length >= 4 && cols(3).nonEmpty) cols(3) else "N/A"
      FileDml(cols(0), cols(1), cols(2), countryValue)
    }

    printSection("RDD dopo parsing in case class FileDml")
    inputColumns.take(20).foreach(println)
    println(s"Righe totali lette dal file: ${inputColumns.count()}")

    // toDF crea un DataFrame usando i nomi dei campi della case class come colonne.
    // cache evita di ricalcolare il DataFrame nelle molte action didattiche successive.
    val df = inputColumns.toDF().cache()
    showDataFrameDetails("DataFrame creato da case class FileDml", df)
    showLanguageSummary("Riepilogo lingue nel DataFrame da case class", df)

    val selectedDf = df.select("state", "capital", "language", "country")
    showDataFrameDetails("Selezione colonne: state, capital, language, country", selectedDf)

    val englishDf = df.filter($"language" === "English")
    showDataFrameDetails("Filtro con DSL: language = English", englishDf)

    df.createOrReplaceTempView("country_table")
    val hindiSqlDf = spark.sql("SELECT state, capital, language, country FROM country_table WHERE language = 'Hindi'")
    showDataFrameDetails("Query SQL su temp view: language = Hindi", hindiSqlDf)

    val outputPath = "C:\\repository\\spark\\2.outputEnglish_1"
    saveCsvIfPossible(sc, englishDf, outputPath)

    // Secondo approccio: Row + StructType.
    // Qui i dati sono generici Row, quindi lo schema viene costruito esplicitamente.
    val rowRdd = inputSplit.map { cols =>
      val countryValue = if (cols.length >= 4 && cols(3).nonEmpty) cols(3) else "N/A"
      Row(cols(0), cols(1), cols(2), countryValue)
    }

    val structSchema = StructType(
      List(
        StructField("state", StringType, nullable = true),
        StructField("capital", StringType, nullable = true),
        StructField("language", StringType, nullable = true),
        StructField("country", StringType, nullable = true)
      )
    )

    val structDf = spark.createDataFrame(rowRdd, structSchema).cache()
    showDataFrameDetails("DataFrame creato con Row + StructType", structDf)
    showLanguageSummary("Riepilogo lingue nel DataFrame Row + StructType", structDf)

    showDataFrameDetails(
      "Select su structDf: state, capital",
      structDf.select("state", "capital")
    )

    showDataFrameDetails(
      "Filtro con DSL su structDf: language = English",
      structDf.filter($"language" === "English")
    )

    structDf.createOrReplaceTempView("struct_country")
    val structHindiSqlDf = spark.sql("SELECT state, capital, language, country FROM struct_country WHERE language = 'Hindi' LIMIT 2")
    showDataFrameDetails("Query SQL su temp view Row DataFrame: language = Hindi LIMIT 2", structHindiSqlDf)

    spark.stop()
  }
}
