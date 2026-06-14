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
// Esempio prima/dopo
// ------------------
// Prima, riga CSV senza header:
// Andhra Pradesh,Amaravati,Telugu,IND
//
// Dopo schema manuale:
// state          | capital   | language | cntry_cd
// Andhra Pradesh | Amaravati | Telugu   | IND
//
// Dopo trim:
// cntry_cd="US   " diventa cntry_cd="US"
//
// Dopo partitionBy("cntry_cd", "language"):
// apply_output_by_country_language/cntry_cd=IND/language=Telugu/part-*.csv
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

  private def printExplanation(text: String): Unit = {
    text.stripMargin.trim.linesIterator.foreach(line => println(line.trim))
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
    printSection("AVVIO - Applicazione trasformazioni su CSV senza header")
    printExplanation(
      """
        |Questo script mostra un flusso completo:
        |1. legge piu file CSV senza intestazione;
        |2. assegna uno schema manuale;
        |3. controlla i dati caricati;
        |4. pulisce valori testuali con trim;
        |5. scrive output CSV normale e output CSV partizionato.
        |
        |L'obiettivo non e' solo produrre file, ma capire come Spark passa da dati grezzi
        |a un DataFrame strutturato e poi a directory di output organizzate.
      """
    )

    // 1. Configurazione iniziale di Spark.
    //
    // Prima di leggere o scrivere dati, dobbiamo creare l'ambiente Spark.
    // In una applicazione Spark ci sono due concetti importanti:
    //
    // - SparkConf:
    //   contiene le impostazioni base del job, per esempio il nome
    //   dell'applicazione e il master da usare.
    //
    // - SparkSession:
    //   e' l'oggetto principale usato nelle API moderne di Spark SQL.
    //   Con SparkSession possiamo leggere CSV, JSON, Parquet, creare DataFrame,
    //   applicare trasformazioni e scrivere output.
    //
    // Qui usiamo una configurazione locale, adatta agli esercizi:
    // - setAppName("depl") assegna un nome all'applicazione Spark;
    // - setMaster("local[*]") esegue Spark in locale usando tutti i core disponibili.
    //
    // In un cluster reale il master non sarebbe local[*], ma un cluster manager
    // come YARN, Kubernetes, Mesos o Spark standalone.
    val conf = new SparkConf().setAppName("depl").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    printSection("1 - Configurazione Spark")
    printExplanation(
      """
        |Creo SparkConf e SparkSession.
        |SparkConf contiene le impostazioni del job, mentre SparkSession e' l'oggetto
        |principale per leggere CSV, creare DataFrame, applicare trasformazioni e scrivere output.
        |
        |Uso master=local[*], quindi il programma gira in locale usando tutti i core disponibili.
        |Il livello di log viene impostato a ERROR per non riempire il terminale con messaggi INFO.
      """
    )

    // Importa le conversioni implicite di Spark.
    //
    // Questa riga abilita alcune sintassi comode sui DataFrame.
    // In questo script serve soprattutto per usare $"nome_colonna", per esempio
    // trim($"cntry_cd"). Senza questo import dovremmo scrivere col("cntry_cd")
    // importando org.apache.spark.sql.functions.col.
    import spark.implicits._

    printSection("2 - Definizione schema manuale")
    printExplanation(
      """
        |I file countries1.txt e countries2.txt non hanno header.
        |Questo significa che la prima riga e' gia' un dato reale, non il nome delle colonne.
        |
        |Senza schema Spark userebbe nomi generici come _c0, _c1, _c2 e _c3.
        |Con StructType assegniamo invece nomi significativi:
        |state, capital, language e cntry_cd.
      """
    )

    // 2. Definizione dello schema manuale.
    //
    // I file countries1.txt e countries2.txt sono CSV senza intestazione:
    //
    // Alaska,Juneau,English,US
    // Andhra Pradesh,Amaravati,Telugu,IND
    //
    // La prima riga non contiene "state,capital,language,cntry_cd".
    // Contiene gia' un record reale. Questo significa che Spark non puo' sapere
    // da solo come chiamare le colonne.
    //
    // Se leggessimo il file senza schema e senza header, Spark creerebbe colonne
    // generiche:
    //
    // _c0, _c1, _c2, _c3
    //
    // Funzionerebbe comunque, ma il codice sarebbe meno leggibile. Per esempio
    // dovremmo scrivere groupBy("_c3") invece di groupBy("cntry_cd").
    //
    // Con StructType definiamo noi la struttura del file:
    // - state: nome dello stato/regione;
    // - capital: capitale;
    // - language: lingua;
    // - cntry_cd: codice paese.
    //
    // StringType indica che leggiamo tutti i campi come stringhe.
    // true indica che il campo puo' contenere valori null.
    //
    // StructType e' quindi lo schema completo del DataFrame.
    // Ogni StructField e' una colonna.
    val dml = StructType(Array(
      StructField("state", StringType, true),
      StructField("capital", StringType, true),
      StructField("language", StringType, true),
      StructField("cntry_cd", StringType, true)
    ))
    println("Schema manuale che verra' applicato:")
    println(dml.treeString)

    // 3. Lettura dei CSV senza header.
    //
    // Qui avviene la lettura vera e propria dei file.
    // spark.read costruisce un DataFrameReader, cioe' l'oggetto Spark usato per
    // leggere dati esterni.
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
    // Questa e' una cosa importante: Spark non sta leggendo un singolo file.
    // Sta leggendo un insieme di file compatibili tra loro, cioe' file con lo
    // stesso numero di colonne e lo stesso significato delle colonne.
    //
    // Il risultato e' un solo DataFrame logico che contiene sia le righe del
    // file con i dati degli Stati Uniti sia le righe del file con i dati
    // dell'India.
    //
    // cache() mantiene il DataFrame in memoria dopo la prima action. Qui e'
    // utile perche' lo stesso df viene usato piu volte: show, groupBy e write.
    //
    // Nota: cache() non esegue subito la lettura. Spark e' lazy:
    // la lettura parte davvero solo quando arriva una action, per esempio
    // count(), show() o write.save().
    printSection("3 - Lettura dei file CSV")
    printExplanation(
      """
        |Spark legge tutti i file nella cartella 1.input\country che iniziano con countries.
        |Il wildcard countries* permette di caricare insieme countries1.txt e countries2.txt.
        |
        |L'opzione header=false dice che non esiste una riga di intestazione.
        |Lo schema dml viene applicato durante la lettura, quindi il DataFrame nasce gia'
        |con colonne leggibili.
        |
        |cache() indica a Spark di mantenere il DataFrame in memoria dopo la prima action,
        |perche' lo useremo piu volte: show, groupBy e scritture su disco.
      """
    )
    val df = spark.read
      .option("header", "false")
      .schema(dml)
      .csv("C:\\repository\\spark\\1.input\\country\\countries*")
      .cache()

    showDataFrameDetails("File country letto senza header con schema manuale", df)

    // 4. Controllo iniziale dei record per codice paese.
    //
    // Questa fase serve come controllo qualitativo prima di modificare o
    // scrivere i dati.
    //
    // Vogliamo capire quanti record abbiamo per ogni codice paese.
    // Nel dataset ci aspettiamo principalmente:
    //
    // - IND: record relativi all'India;
    // - US: record relativi agli Stati Uniti.
    //
    // groupBy("cntry_cd") raggruppa le righe per codice paese.
    // count() conta quante righe ci sono per ogni gruppo.
    // orderBy("cntry_cd") ordina il risultato, cosi l'output e' piu leggibile.
    //
    // Questa stampa avviene prima del trim, quindi se nei file ci sono valori
    // come "US" e "US   ", Spark li considera codici paese diversi.
    //
    // Questo e' un esempio reale di problema sui dati: a video due valori
    // sembrano uguali, ma per Spark non lo sono perche' contengono spazi
    // finali. La fase successiva serve proprio a correggere questo problema.
    printSection("4 - Riepilogo per codice paese prima della pulizia")
    printExplanation(
      """
        |Prima di scrivere i dati controlliamo quanti record ci sono per ogni codice paese.
        |Questa verifica e' utile per scoprire problemi di qualita' sui dati.
        |
        |Se nel file esistono valori come "US" e "US   ", Spark li considera diversi.
        |Per questo motivo il riepilogo prima del trim puo' mostrare piu righe che sembrano
        |riferirsi allo stesso paese.
      """
    )
    df.groupBy("cntry_cd").count().orderBy("cntry_cd").show(MaxRowsToShow, truncate = false)

    // 5. Scrittura completa in una sola directory.
    //
    // Questa prima scrittura salva il DataFrame completo senza partizionarlo
    // per colonne. Tutti i record finiscono nella stessa directory di output.
    //
    // Spark non scrive normalmente un singolo file finale con un nome scelto da
    // noi. Scrive una directory, e dentro quella directory crea file come:
    //
    // part-00000-....csv
    // _SUCCESS
    //
    // Il file _SUCCESS indica che la scrittura e' terminata correttamente.
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
    //
    // Questa scrittura e' utile quando vuoi un output semplice da aprire e
    // controllare manualmente, senza cartelle divise per paese o lingua.
    printSection("5 - Scrittura completa in una sola directory")
    printExplanation(
      """
        |Ora salvo il DataFrame completo in formato CSV.
        |Spark non salva un singolo file con un nome scelto da noi: crea una directory
        |di output che contiene uno o piu file part-*.csv e un file tecnico _SUCCESS.
        |
        |Uso coalesce(1) per ottenere un solo file part-*.csv. E' comodo per gli esercizi,
        |ma su dataset grandi puo' essere inefficiente perche' forza i dati in una sola partizione.
        |
        |Uso mode=overwrite, quindi se la cartella di output esiste gia' viene sostituita.
      """
    )
    println("Strategia: coalesce(1), formato CSV, mode overwrite")
    println("Destinazione: output_one_dir")
    df.coalesce(1)
      .write
      .mode("overwrite")
      .format("csv")
      .save("C:\\repository\\spark\\2.output\\apply_output_one_dir")

    // 6. Pulizia del codice paese.
    //
    // Prima di creare output partizionati conviene pulire i valori usati come
    // colonne di partizione.
    //
    // Se cntry_cd contiene valori non puliti, Spark crea una cartella diversa
    // per ogni valore distinto. Per esempio:
    //
    // cntry_cd=US
    // cntry_cd=US%20%20%20
    //
    // Queste due cartelle rappresenterebbero entrambe gli Stati Uniti, ma Spark
    // le tratterebbe come partizioni diverse perche' il valore testuale e'
    // diverso.
    //
    // trim rimuove spazi iniziali e finali dalla colonna cntry_cd.
    // Questo passaggio e' importante prima di partizionare: se non pulisci i
    // valori, Spark potrebbe creare cartelle diverse per "US" e "US   ".
    //
    // withColumn("cntry_cd", ...) sostituisce la colonna cntry_cd con la sua
    // versione ripulita.
    //
    // Il DataFrame originale df non viene modificato: i DataFrame Spark sono
    // immutabili. withColumn restituisce un nuovo DataFrame, qui chiamato
    // df_clean.
    //
    // Prima:
    // cntry_cd = "US   "
    //
    // Dopo:
    // cntry_cd = "US"
    printSection("6 - Pulizia del codice paese con trim")
    printExplanation(
      """
        |Prima di partizionare per cntry_cd pulisco la colonna con trim.
        |trim rimuove spazi iniziali e finali.
        |
        |Questo passaggio evita cartelle duplicate come cntry_cd=US e cntry_cd=US%20%20%20.
        |Il DataFrame originale non viene modificato: Spark crea un nuovo DataFrame chiamato df_clean.
      """
    )
    val df_clean = df.withColumn("cntry_cd", trim($"cntry_cd")).cache()
    showDataFrameDetails("DataFrame con cntry_cd ripulito tramite trim", df_clean)

    // 7. Scrittura partizionata per codice paese.
    //
    // In questa fase scriviamo lo stesso dataset, ma organizzato fisicamente
    // per codice paese.
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
    //
    // Esempio pratico: se in futuro una query legge solo cntry_cd = "IND",
    // Spark puo' andare direttamente nella cartella cntry_cd=IND invece di
    // scansionare tutti i file.
    //
    // Questa ottimizzazione si chiama partition pruning.
    printSection("7 - Scrittura partizionata per codice paese")
    printExplanation(
      """
        |Qui uso partitionBy("cntry_cd").
        |Il valore di cntry_cd viene usato per creare cartelle separate, una per ogni paese.
        |
        |Output atteso:
        |C:\repository\spark\2.output\apply_output_by_country\cntry_cd=IND
        |C:\repository\spark\2.output\apply_output_by_country\cntry_cd=US
        |
        |Questa organizzazione e' utile quando una lettura futura vuole filtrare solo un paese:
        |Spark puo' saltare le cartelle non rilevanti.
      """
    )
    println("Partizionamento: cntry_cd")
    df_clean.coalesce(1)
      .write
      .mode("overwrite")
      .format("csv")
      .partitionBy("cntry_cd")
      .save("C:\\repository\\spark\\2.output\\apply_output_by_country")

    // 8. Pulizia anche della lingua.
    //
    // Ora vogliamo partizionare non solo per paese, ma anche per lingua.
    // Per questo motivo dobbiamo assicurarci che anche la colonna language sia
    // pulita.
    //
    // Per la doppia partizione useremo sia cntry_cd sia language.
    // Quindi puliamo anche language per evitare cartelle duplicate come
    // language=English e language=English%20%20%20.
    //
    // Anche qui creiamo un nuovo DataFrame, df_clean_lang, senza modificare il
    // DataFrame originale. Questo rende chiaro quale versione dei dati viene
    // usata nella scrittura successiva.
    printSection("8 - Pulizia anche della colonna language")
    printExplanation(
      """
        |Per la prossima scrittura useremo due colonne di partizione: cntry_cd e language.
        |Quindi puliamo anche language con trim.
        |
        |Senza questa pulizia potremmo ottenere cartelle duplicate come:
        |language=English
        |language=English%20%20%20
      """
    )
    val df_clean_lang = df
      .withColumn("cntry_cd", trim($"cntry_cd"))
      .withColumn("language", trim($"language"))
      .cache()
    showDataFrameDetails("DataFrame con cntry_cd e language ripuliti tramite trim", df_clean_lang)

    // 9. Scrittura partizionata per codice paese e lingua.
    //
    // Questa e' la scrittura piu strutturata dell'esempio.
    //
    // Con una sola partizione, Spark crea cartelle solo al primo livello:
    //
    // cntry_cd=IND
    // cntry_cd=US
    //
    // Con due colonne in partitionBy, Spark crea una gerarchia.
    // La prima colonna e' il primo livello della directory.
    // La seconda colonna e' il secondo livello.
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
    //
    // Attenzione: non bisogna scegliere troppe colonne di partizione o colonne
    // con troppi valori distinti. Se una colonna ha migliaia di valori diversi,
    // Spark puo' creare moltissime cartelle piccole, rendendo il dataset piu
    // difficile da gestire.
    printSection("9 - Scrittura partizionata per codice paese e lingua")
    printExplanation(
      """
        |Questa e' la scrittura piu strutturata dell'esempio.
        |Uso partitionBy("cntry_cd", "language"), quindi Spark crea una gerarchia:
        |prima una cartella per paese, poi dentro ogni paese una cartella per lingua.
        |
        |Esempi:
        |apply_output_by_country_language/cntry_cd=IND/language=Hindi
        |apply_output_by_country_language/cntry_cd=IND/language=English
        |apply_output_by_country_language/cntry_cd=US/language=English
        |
        |Questa tecnica e' utile, ma va usata con criterio: troppe colonne di partizione
        |o colonne con troppi valori distinti possono generare troppe cartelle piccole.
      """
    )
    println("Partizionamento: cntry_cd, language")
    df_clean_lang.coalesce(1)
      .write
      .mode("overwrite")
      .format("csv")
      .partitionBy("cntry_cd", "language")
      .save("C:\\repository\\spark\\2.output\\apply_output_by_country_language")

    // Chiude la SparkSession.
    printSection("FINE - Job completato")
    printExplanation(
      """
        |Il job ha generato tre output:
        |1. apply_output_one_dir: output CSV completo in una sola directory;
        |2. apply_output_by_country: output diviso per cntry_cd;
        |3. apply_output_by_country_language: output diviso per cntry_cd e language.
      """
    )
    spark.stop()
  }
}
