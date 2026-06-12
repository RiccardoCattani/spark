// Scopo dello script
// ------------------
// Questo script parte da un CSV senza header, applica uno schema manuale e poi
// mostra alcune trasformazioni tipiche sui DataFrame.
//
// L'obiettivo principale e' dimostrare:
// - come assegnare nomi e tipi alle colonne con StructType;
// - come pulire valori testuali con trim;
// - come scrivere un output CSV normale;
// - come scrivere output partizionati con partitionBy.
//
// Il partizionamento crea cartelle separate in base ai valori delle colonne
// scelte, per esempio cntry_cd oppure cntry_cd + language.
//
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.trim
import org.apache.spark.sql.types._

// Questo script estende l'esempio di lettura senza header:
// definisce uno schema manuale, legge una cartella di CSV, pulisce alcune colonne
// con trim e scrive output partizionati per paese e per lingua.
object ReadingFileWithoutHeader2 {
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
    // Configura Spark in locale e crea la SparkSession.
    val conf = new SparkConf().setAppName("depl").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    // Schema manuale usato per assegnare nomi e tipi alle colonne del file.
    val dml = StructType(Array(
      StructField("state", StringType, true),
      StructField("capital", StringType, true),
      StructField("language", StringType, true),
      StructField("cntry_cd", StringType, true)
    ))

    // Legge il CSV senza header applicando lo schema manuale.
    val df = spark.read
      .option("header", "false")
      .schema(dml)
      .csv("/home/riccardo/Documenti/repository/spark/spark/country")
      .cache()

    showDataFrameDetails("File country letto senza header con schema manuale", df)

    // Conta i record per codice paese prima della pulizia.
    printSection("Riepilogo per codice paese")
    df.groupBy("cntry_cd").count().orderBy("cntry_cd").show(MaxRowsToShow, truncate = false)

    // Scrive tutto l'output in una singola partizione.
    printSection("Scrittura output_one_dir")
    println("Strategia: coalesce(1), formato CSV, mode overwrite")
    println("Destinazione: output_one_dir")
    df.coalesce(1)
      .write
      .mode("overwrite")
      .format("csv")
      .save("file:///home/riccardo/Documenti/repository/spark/spark/output_one_dir")

    // trim rimuove spazi iniziali/finali dal codice paese.
    val df_clean = df.withColumn("cntry_cd", trim($"cntry_cd")).cache()
    showDataFrameDetails("DataFrame con cntry_cd ripulito tramite trim", df_clean)

    // Scrive output partizionato per codice paese.
    // Spark crea una cartella per ogni valore distinto di cntry_cd.
    printSection("Scrittura output_by_country")
    println("Partizionamento: cntry_cd")
    df_clean.coalesce(1)
      .write
      .mode("overwrite")
      .format("csv")
      .partitionBy("cntry_cd")
      .save("file:///home/riccardo/Documenti/repository/spark/spark/output_by_country")

    // Pulisce anche la colonna language prima del partizionamento doppio.
    val df_clean_lang = df
      .withColumn("cntry_cd", trim($"cntry_cd"))
      .withColumn("language", trim($"language"))
      .cache()
    showDataFrameDetails("DataFrame con cntry_cd e language ripuliti tramite trim", df_clean_lang)

    // Scrive output partizionato prima per codice paese e poi per lingua.
    printSection("Scrittura output_by_country_language")
    println("Partizionamento: cntry_cd, language")
    df_clean_lang.coalesce(1)
      .write
      .mode("overwrite")
      .format("csv")
      .partitionBy("cntry_cd", "language")
      .save("file:///home/riccardo/Documenti/repository/spark/spark/output_by_country_language")

    // Chiude la SparkSession.
    spark.stop()
  }
}
