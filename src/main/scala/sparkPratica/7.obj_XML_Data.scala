package sparkPractise

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

// Questo esempio mostra come usare Spark per scrivere e leggere dati in formato XML.
// Spark non gestisce XML con le sole librerie base: per questo progetto viene usata
// la dipendenza com.databricks:spark-xml, gia' presente nel file build.sbt.
//
// Il flusso dello script e':
// 1. legge il file India_pipe.txt come CSV con delimitatore pipe;
// 2. mostra schema e prime righe del DataFrame;
// 3. scrive i dati in formato XML;
// 4. rilegge il file XML generato;
// 5. mostra i dati XML caricati nuovamente in Spark.
object obj_XML_Data {
  def main(arg: Array[String]): Unit = {
    // Percorso del file CSV di input.
    // Se non viene passato nessun argomento da riga di comando, usa India_pipe.txt.
    val inputPath = arg.headOption.getOrElse("India_pipe.txt")

    // Percorso della cartella di output dove Spark scrivera' i file XML generati.
    // Spark scrive sempre una cartella, non un singolo file.
    val outputPath = arg.lift(1).getOrElse("2.output/xml_data")

    // Percorso del file XML gia' esistente da leggere.
    // In questo progetto books.xml si trova nella cartella 1.input.
    val booksXmlPath = arg.lift(2).getOrElse("1.input/books.xml")

    // Crea la configurazione base di Spark.
    // setAppName assegna un nome al job.
    // setMaster("local[*]") esegue Spark in locale usando tutti i core disponibili.
    val conf = new SparkConf()
      .setAppName("job1")
      .setMaster("local[*]")

    // Crea lo SparkContext, cioe' il punto di ingresso basso livello di Spark.
    // Serve per inizializzare l'applicazione Spark.
    val sc = new SparkContext(conf)

    // Riduce i log mostrati in console, lasciando visibili soprattutto gli errori.
    sc.setLogLevel("Error")

    // Crea la SparkSession, cioe' il punto di ingresso principale per lavorare
    // con DataFrame, SQL e lettura/scrittura di file strutturati.
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    // Importa le conversioni implicite di Spark.
    // Sono utili quando si lavora con DataFrame, Dataset e colonne.
    import spark.implicits._

    // Prima parte: lettura diretta di un file XML gia' esistente.
    println("************Read XML file************")

    // Rimuove l'eventuale prefisso file:/// per poter controllare l'esistenza
    // del file con java.io.File.
    val localBooksXmlPath = booksXmlPath.stripPrefix("file:///")

    // Controlla se books.xml esiste prima di provare a leggerlo.
    // In questo modo lo script non va in errore se il file manca.
    if (new File(localBooksXmlPath).exists()) {
      // Legge il file XML con la libreria spark-xml.
      // rowTag = "book" indica che ogni tag <book>...</book> diventa una riga
      // del DataFrame.
      val readXmlFile = spark.read
        .format("com.databricks.spark.xml")
        .option("rowTag", "book")
        .load(booksXmlPath)

      // Stampa lo schema ricavato dal file XML: colonne, tipi e nullable.
      readXmlFile.printSchema()

      // Mostra i dati letti dal file XML.
      // truncate = false evita di tagliare i testi lunghi.
      readXmlFile.show(truncate = false)
    } else {
      // Messaggio mostrato solo se books.xml non viene trovato.
      println(s"File XML non trovato: $booksXmlPath")
      println("La lettura diretta di books.xml viene saltata.")
    }

    // Seconda parte: lettura di un file CSV e conversione in XML.
    println("************Reading CSV Data************")

    // Legge il file CSV.
    // header = true indica che la prima riga contiene i nomi delle colonne.
    // delimiter = "|" indica che i campi sono separati dal carattere pipe.
    val readCsv = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", "|")
      .load(inputPath)

    // Mostra lo schema del DataFrame ottenuto dal CSV.
    println("************Schema del DataFrame letto dal CSV************")
    readCsv.printSchema()

    // Mostra le prime 10 righe del CSV caricato in Spark.
    // Serve per verificare che header e delimitatore siano stati interpretati bene.
    println("************Prime 10 righe lette dal CSV************")
    readCsv.show(10, truncate = false)

    // Scrive il DataFrame in formato XML.
    println("************Writing XML Data************")
    readCsv.write
      // Usa il formato XML tramite la libreria spark-xml.
      .format("xml")
      // rootTag definisce il tag radice del documento XML.
      // L'output avra' una struttura simile a <records>...</records>.
      .option("rootTag", "records")
      // rowTag definisce il tag usato per ogni riga del DataFrame.
      // Ogni record sara' scritto come <record>...</record>.
      .option("rowTag", "record")
      // overwrite sovrascrive la cartella di output se esiste gia'.
      .mode("overwrite")
      // Salva l'XML nella cartella indicata da outputPath.
      .save(outputPath)

    // Terza parte: rilettura dell'XML appena generato.
    println("************Reading XML Data************")

    // Legge la cartella XML prodotta nel passaggio precedente.
    // Usa rowTag = "record" perche' in scrittura ogni riga e' stata salvata
    // dentro un tag <record>.
    val readXml = spark.read
      .format("xml")
      .option("rowTag", "record")
      .load(outputPath)

    // Stampa lo schema dell'XML riletto.
    readXml.printSchema()

    // Mostra i dati letti dall'XML generato.
    readXml.show(truncate = false)

    // Chiude la SparkSession e libera le risorse usate da Spark SQL/DataFrame.
    spark.stop()

    // Chiude lo SparkContext e termina l'applicazione Spark.
    sc.stop()
  }
}
