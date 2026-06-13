package sparkPratica
// sbt "runMain sparkPratica.obj_apply_transformation"
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
object obj_apply_transformation {
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
    // 1. Configurazione iniziale di Spark.
    //
    // SparkConf contiene le impostazioni base del job:
    // - setAppName("depl") assegna un nome all'applicazione Spark;
    // - setMaster("local[*]") esegue Spark in locale usando tutti i core disponibili.
    //
    // SparkSession e' il punto di ingresso principale per lavorare con DataFrame,
    // leggere file strutturati e scrivere output in vari formati.
    val conf = new SparkConf().setAppName("depl").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // Importa le conversioni implicite di Spark.
    // In questo script serve per usare la sintassi $"nome_colonna" dentro trim.
    import spark.implicits._

    // 2. Definizione dello schema manuale.
    //
    // I file countries1.txt e countries2.txt non hanno una riga di intestazione.
    // Senza schema, Spark assegnerebbe nomi generici come _c0, _c1, _c2, _c3.
    //
    // Con StructType definiamo noi la struttura del file:
    // - state: nome dello stato/regione;
    // - capital: capitale;
    // - language: lingua;
    // - cntry_cd: codice paese.
    //
    // StringType indica che leggiamo tutti i campi come stringhe.
    // true indica che il campo puo' contenere valori null.
    val dml = StructType(Array(
      StructField("state", StringType, true),
      StructField("capital", StringType, true),
      StructField("language", StringType, true),
      StructField("cntry_cd", StringType, true)
    ))

    // 3. Lettura dei CSV senza header.
    //
    // option("header", "false") dice a Spark che la prima riga non contiene i
    // nomi delle colonne, ma e' una normale riga dati.
    //
    // schema(dml) applica lo schema manuale definito sopra.
    //
    // Il path finisce con countries*: questo wildcard fa leggere a Spark tutti i
    // file nella cartella 1.input\country che iniziano con "countries".
    // In pratica vengono caricati insieme countries1.txt e countries2.txt.
    //
    // cache() mantiene il DataFrame in memoria dopo la prima action. Qui e'
    // utile perche' lo stesso df viene usato piu volte: show, groupBy e write.
    val df = spark.read
      .option("header", "false")
      .schema(dml)
      .csv("C:\\repository\\spark\\1.input\\country\\countries*")
      .cache()

    showDataFrameDetails("File country letto senza header con schema manuale", df)

    // 4. Controllo iniziale dei record per codice paese.
    //
    // groupBy("cntry_cd") raggruppa le righe per codice paese.
    // count() conta quante righe ci sono per ogni gruppo.
    // orderBy("cntry_cd") ordina il risultato, cosi l'output e' piu leggibile.
    //
    // Questa stampa avviene prima del trim, quindi se nei file ci sono valori
    // come "US" e "US   ", Spark li considera codici paese diversi.
    printSection("Riepilogo per codice paese")
    df.groupBy("cntry_cd").count().orderBy("cntry_cd").show(MaxRowsToShow, truncate = false)

    // 5. Scrittura completa in una sola directory.
    //
    // coalesce(1) riduce il DataFrame a una sola partizione prima della scrittura.
    // Questo produce un solo file part-*.csv nella directory di output.
    //
    // Nota pratica: coalesce(1) e' comodo negli esercizi per avere un solo file,
    // ma su dataset grandi puo' essere inefficiente perche' concentra tutto su
    // una sola partizione.
    //
    // mode("overwrite") cancella l'output precedente se la cartella esiste gia'.
    // format("csv") indica che vogliamo scrivere in formato CSV.
    printSection("Scrittura output_one_dir")
    println("Strategia: coalesce(1), formato CSV, mode overwrite")
    println("Destinazione: output_one_dir")
    df.coalesce(1)
      .write
      .mode("overwrite")
      .format("csv")
      .save("C:\\repository\\spark\\2.output\\apply_output_one_dir")

    // 6. Pulizia del codice paese.
    //
    // trim rimuove spazi iniziali e finali dalla colonna cntry_cd.
    // Questo passaggio e' importante prima di partizionare: se non pulisci i
    // valori, Spark potrebbe creare cartelle diverse per "US" e "US   ".
    //
    // withColumn("cntry_cd", ...) sostituisce la colonna cntry_cd con la sua
    // versione ripulita.
    val df_clean = df.withColumn("cntry_cd", trim($"cntry_cd")).cache()
    showDataFrameDetails("DataFrame con cntry_cd ripulito tramite trim", df_clean)

    // 7. Scrittura partizionata per codice paese.
    //
    // partitionBy("cntry_cd") non scrive il valore cntry_cd dentro il file CSV:
    // lo usa per creare cartelle separate.
    //
    // L'output avra' una struttura simile a:
    // apply_output_by_country/cntry_cd=IND/part-00000-....csv
    // apply_output_by_country/cntry_cd=US/part-00000-....csv
    //
    // Questo e' utile quando vuoi leggere o analizzare solo un sottoinsieme dei
    // dati, per esempio solo il paese IND o solo il paese US.
    printSection("Scrittura output_by_country")
    println("Partizionamento: cntry_cd")
    df_clean.coalesce(1)
      .write
      .mode("overwrite")
      .format("csv")
      .partitionBy("cntry_cd")
      .save("C:\\repository\\spark\\2.output\\apply_output_by_country")

    // 8. Pulizia anche della lingua.
    //
    // Per la doppia partizione useremo sia cntry_cd sia language.
    // Quindi puliamo anche language per evitare cartelle duplicate come
    // language=English e language=English%20%20%20.
    val df_clean_lang = df
      .withColumn("cntry_cd", trim($"cntry_cd"))
      .withColumn("language", trim($"language"))
      .cache()
    showDataFrameDetails("DataFrame con cntry_cd e language ripuliti tramite trim", df_clean_lang)

    // 9. Scrittura partizionata per codice paese e lingua.
    //
    // partitionBy("cntry_cd", "language") crea una gerarchia di cartelle:
    // prima una cartella per paese, poi dentro ogni paese una cartella per lingua.
    //
    // Esempi di output:
    // apply_output_by_country_language/cntry_cd=IND/language=Hindi/part-...
    // apply_output_by_country_language/cntry_cd=IND/language=English/part-...
    // apply_output_by_country_language/cntry_cd=US/language=English/part-...
    //
    // Questa tecnica e' utile per dataset grandi, perche' permette a Spark di
    // saltare intere cartelle quando una query filtra per paese o lingua.
    printSection("Scrittura output_by_country_language")
    println("Partizionamento: cntry_cd, language")
    df_clean_lang.coalesce(1)
      .write
      .mode("overwrite")
      .format("csv")
      .partitionBy("cntry_cd", "language")
      .save("C:\\repository\\spark\\2.output\\apply_output_by_country_language")

    // Chiude la SparkSession.
    spark.stop()
  }
}
