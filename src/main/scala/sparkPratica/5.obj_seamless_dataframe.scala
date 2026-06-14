/*
  Esecuzione:
    sbt "runMain obj_SeamlessDataframe"

  Scopo dello script
  ------------------
  Questo script e' un esempio pratico di lavoro "seamless" con i DataFrame Spark:
  mostra come Spark possa leggere dati da un formato sorgente, rappresentarli come
  DataFrame e poi scriverli in formati diversi senza cambiare il modello logico dei dati.

  In particolare lo script dimostra:
    1. Lettura di un file CSV classico separato da virgola.
    2. Lettura di un file CSV separato da pipe, cioe' con delimitatore "|".
    3. Ispezione dettagliata dei DataFrame letti:
       - numero di colonne;
       - elenco dei nomi colonna;
       - numero totale di righe;
       - schema Spark;
       - dati mostrati in console, fino a un massimo configurato.
    4. Scrittura dello stesso DataFrame in piu formati:
       - CSV con delimitatore diverso, in questo caso "~";
       - ORC;
       - Parquet;
       - JSON.
    5. Rilettura degli output ORC, Parquet e JSON per verificare che Spark riesca
       a ricostruire correttamente i DataFrame dai file generati.

  Concetto importante
  -------------------
  Il punto centrale e' che il DataFrame fa da rappresentazione comune dei dati.
  Una volta letto il file `India_pipe.txt` come DataFrame, lo stesso contenuto puo'
  essere salvato in formati fisici diversi. Cambia il formato su disco, ma le operazioni
  Spark rimangono molto simili: `read.format(...).load(...)` per leggere e
  `write.format(...).save(...)` per scrivere.

  Differenza tra i formati usati
  ------------------------------
  CSV:
    Formato testuale, facile da aprire e leggere manualmente. Non conserva bene i tipi
    complessi e di solito richiede opzioni come `header` e `delimiter`.

  ORC:
    Formato colonnare ottimizzato per query analitiche. E' efficiente per leggere solo
    alcune colonne e per comprimere grandi dataset.

  Parquet:
    Altro formato colonnare molto diffuso nell'ecosistema big data. E' usato spesso con
    Spark, Hive, Impala, Presto/Trino e data lake moderni.

  JSON:
    Formato testuale semi-strutturato. E' piu flessibile del CSV per dati annidati,
    ma in genere meno efficiente di ORC/Parquet per analisi massive.

  Dettagli sull'output in console
  -------------------------------
  La funzione `showDataFrameDetails` evita stampe troppo povere come un semplice `show(5)`.
  Per ogni DataFrame stampa una sezione leggibile con conteggio righe, colonne, schema
  e dati di esempio. Se il DataFrame contiene piu di 100 righe, lo script mostra solo
  le prime 100 per non riempire troppo la console.

  Percorsi usati
  --------------
  I path sono locali Windows e puntano a `C:/repository/spark/...`.
  Per eseguire lo script senza modifiche devono esistere:
    - file:///C:/repository/spark/1.input/train.csv
    - file:///C:/repository/spark/India_pipe.txt

  Gli output vengono creati sotto:
    - file:///C:/repository/spark/2.output/file_india_csv
    - file:///C:/repository/spark/2.output/file_india_orc
    - file:///C:/repository/spark/2.output/file_india_parquet
    - file:///C:/repository/spark/2.output/file_india_json

  Nota su mode("overwrite")
  -------------------------
  Tutte le scritture usano `mode("overwrite")`: se la cartella di destinazione esiste
  gia', Spark la sostituisce. Questo e' comodo negli esercizi, ma in produzione va usato
  con attenzione per evitare di cancellare dati utili.

  Esempio output atteso
  ---------------------
  Prima, India_pipe.txt:
    State|Capital|Language
    Kerala|Thiruvananthapuram|Malayalam

  Dopo lettura:
    DataFrame con colonne State, Capital e Language.

  Dopo scrittura CSV con delimiter "~":
    State~Capital~Language
    Kerala~Thiruvananthapuram~Malayalam

  Dopo scrittura ORC/Parquet/JSON:
    vengono create directory file_india_orc, file_india_parquet e file_india_json
    contenenti file part-* nel formato scelto.
*/

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

object obj_SeamlessDataframe {
  private val MaxRowsToShow = 100
  private val WriteMode = "overwrite"

  private val TrainCsvPath = "file:///C:/repository/spark/1.input/train.csv"
  private val IndiaPipePath = "file:///C:/repository/spark/India_pipe.txt"

  private val OutputBasePath = "file:///C:/repository/spark/2.output"
  private val CsvOutputPath = s"$OutputBasePath/file_india_csv"
  private val OrcOutputPath = s"$OutputBasePath/file_india_orc"
  private val ParquetOutputPath = s"$OutputBasePath/file_india_parquet"
  private val JsonOutputPath = s"$OutputBasePath/file_india_json"

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

  private def printWriteDetails(format: String, mode: String, destination: String, extra: String = ""): Unit = {
    printSection(s"Scrittura formato $format")
    println(s"Formato: $format")
    println(s"Modalita': $mode")
    println(s"Destinazione: $destination")
    if (extra.nonEmpty) {
      println(extra)
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Job1")
      .setMaster("local[*]")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val read_csv_df = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load(TrainCsvPath)
      .cache()
    showDataFrameDetails("Lettura CSV train.csv", read_csv_df)

    val read_pipe_df = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", "|")
      .load(IndiaPipePath)
      .cache()
    showDataFrameDetails("Lettura CSV pipe-delimited India_pipe.txt", read_pipe_df)

    printWriteDetails(
      format = "csv",
      mode = WriteMode,
      destination = CsvOutputPath,
      extra = "Header: true | Delimiter output: ~"
    )
    println("Prima: colonne separate da pipe. Dopo: output CSV con colonne separate da ~.")
    read_pipe_df.write
      .format("csv")
      .option("header", "true")
      .option("delimiter", "~")
      .mode(WriteMode)
      .save(CsvOutputPath)
    println("Scrittura CSV completata.")

    printWriteDetails(
      format = "orc",
      mode = WriteMode,
      destination = OrcOutputPath
    )
    read_pipe_df.write
      .format("orc")
      .mode(WriteMode)
      .save(OrcOutputPath)
    println("Scrittura ORC completata.")

    printWriteDetails(
      format = "parquet",
      mode = WriteMode,
      destination = ParquetOutputPath
    )
    read_pipe_df.write
      .format("parquet")
      .mode(WriteMode)
      .save(ParquetOutputPath)
    println("Scrittura Parquet completata.")

    printWriteDetails(
      format = "json",
      mode = WriteMode,
      destination = JsonOutputPath
    )
    read_pipe_df.write
      .format("json")
      .mode(WriteMode)
      .save(JsonOutputPath)
    println("Scrittura JSON completata.")

    val orc_df = spark.read
      .format("orc")
      .load(OrcOutputPath)
    showDataFrameDetails("Rilettura output ORC", orc_df)

    val parquet_df = spark.read
      .format("parquet")
      .load(ParquetOutputPath)
    showDataFrameDetails("Rilettura output Parquet", parquet_df)

    val json_df = spark.read
      .format("json")
      .load(JsonOutputPath)
    showDataFrameDetails("Rilettura output JSON", json_df)

    spark.stop()
  }
}
