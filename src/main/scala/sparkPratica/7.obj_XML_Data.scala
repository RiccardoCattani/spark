package sparkPractise

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

// Scopo dello script
// ------------------
// Questo script dimostra come Apache Spark puo' lavorare con dati in formato XML
// usando la libreria esterna com.databricks:spark-xml, gia' configurata nel file
// build.sbt. Spark, infatti, non legge e scrive XML con le sole librerie base:
// per usare .format("xml") o .format("com.databricks.spark.xml") serve questa
// dipendenza aggiuntiva.
//
// L'obiettivo e' mostrare due casi pratici:
//
// 1. Lettura di un XML gia' esistente
//    Lo script legge il file 1.input/books.xml. In questo file ogni libro e'
//    rappresentato dal tag <book>...</book>. Con l'opzione rowTag = "book",
//    Spark interpreta ogni elemento <book> come una riga di un DataFrame.
//    In questo modo un file XML gerarchico viene trasformato in una tabella
//    con colonne come id, author, title, genre, price e publish_date.
//
// 2. Conversione da CSV a XML
//    Lo script legge India_pipe.txt come CSV separato dal carattere pipe "|",
//    lo carica in un DataFrame e poi salva lo stesso contenuto in formato XML.
//    Il DataFrame fa quindi da formato intermedio comune:
//
//      CSV -> DataFrame -> XML
//
//    Dopo la scrittura, lo script rilegge anche l'XML generato per verificare
//    che l'output sia corretto e nuovamente utilizzabile da Spark.
//
// In sintesi, vogliamo dimostrare che Spark puo':
// - leggere un file XML gia' pronto e trasformarlo in DataFrame;
// - leggere un CSV e convertirlo in XML;
// - rileggere l'XML generato come controllo finale;
// - usare il DataFrame come rappresentazione comune tra formati diversi.
//
// Esempio prima/dopo
// ------------------
// XML input:
// <book><author>Gambardella</author><title>XML Developer's Guide</title></book>
//
// Dopo lettura con rowTag="book":
// author      | title
// Gambardella | XML Developer's Guide
//
// CSV -> DataFrame -> XML:
// state|capital|language
// Kerala|Thiruvananthapuram|Malayalam
//
// diventa:
// <record><state>Kerala</state><capital>Thiruvananthapuram</capital><language>Malayalam</language></record>
object obj_XML_Data {
  private def printSection(title: String): Unit = {
    println()
    println("=" * 90)
    println(title)
    println("=" * 90)
  }

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

    printSection("AVVIO - Lettura XML e conversione CSV in XML")
    println("Obiettivo: leggere un XML esistente, convertire un CSV in XML e rileggere l'output.")
    println(s"CSV input: $inputPath")
    println(s"XML output: $outputPath")
    println(s"XML esistente da leggere: $booksXmlPath")

    // Crea la configurazione base di Spark.
    // setAppName assegna un nome al job.
    // setMaster("local[*]") esegue Spark in locale usando tutti i core disponibili.
    val conf = new SparkConf()
      .setAppName("job1")
      .setMaster("local[*]")

    printSection("1 - Configurazione Spark")
    println("Creo SparkConf con appName=job1 e master=local[*].")

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
    printSection("2 - Lettura file XML esistente")
    println("Spark legge il file XML usando format=com.databricks.spark.xml.")
    println("L'opzione rowTag=book indica che ogni tag <book> diventa una riga.")
    println(s"Path input XML: $booksXmlPath")

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

      println(s"Numero righe XML lette: ${readXmlFile.count()}")
      println("Schema ricavato dal file XML:")
      readXmlFile.printSchema()

      // Mostra i dati letti dal file XML.
      // truncate = false evita di tagliare i testi lunghi.
      println("Dati letti dal file XML:")
      readXmlFile.show(truncate = false)
    } else {
      // Messaggio mostrato solo se books.xml non viene trovato.
      println(s"File XML non trovato: $booksXmlPath")
      println("La lettura diretta di books.xml viene saltata.")
    }

    // Seconda parte: lettura di un file CSV e conversione in XML.
    printSection("3 - Lettura CSV da convertire in XML")
    println("Spark legge il CSV con header=true e delimiter=|.")
    println(s"Path input CSV: $inputPath")

    // Legge il file CSV.
    // header = true indica che la prima riga contiene i nomi delle colonne.
    // delimiter = "|" indica che i campi sono separati dal carattere pipe.
    val readCsv = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", "|")
      .load(inputPath)

    // Mostra lo schema del DataFrame ottenuto dal CSV.
    println(s"Numero righe CSV lette: ${readCsv.count()}")
    println("Schema del DataFrame letto dal CSV:")
    readCsv.printSchema()

    // Mostra le prime 10 righe del CSV caricato in Spark.
    // Serve per verificare che header e delimitatore siano stati interpretati bene.
    println("Esempio: una riga CSV diventa una riga del DataFrame con colonne nominate.")
    println("Prime 10 righe lette dal CSV:")
    readCsv.show(10, truncate = false)

    // Scrive il DataFrame in formato XML.
    printSection("4 - Scrittura DataFrame in formato XML")
    println("Spark scrive il DataFrame in XML con rootTag=records e rowTag=record.")
    println("Mode: overwrite")
    println(s"Path output XML: $outputPath")
    println("Output atteso: <records> con un tag <record> per ogni riga del DataFrame.")
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
    printSection("5 - Rilettura XML generato")
    println("Spark rilegge l'XML appena scritto per verificare l'output.")
    println("L'opzione rowTag=record indica che ogni tag <record> diventa una riga.")
    println(s"Path XML generato: $outputPath")

    // Legge la cartella XML prodotta nel passaggio precedente.
    // Usa rowTag = "record" perche' in scrittura ogni riga e' stata salvata
    // dentro un tag <record>.
    val readXml = spark.read
      .format("xml")
      .option("rowTag", "record")
      .load(outputPath)

    println(s"Numero righe XML generate lette: ${readXml.count()}")
    println("Schema dell'XML riletto:")
    readXml.printSchema()

    // Mostra i dati letti dall'XML generato.
    println("Dati letti dall'XML generato:")
    readXml.show(truncate = false)

    printSection("FINE - Job completato")
    println("Sono state eseguite lettura XML, conversione CSV -> XML e verifica dell'output.")

    // Chiude la SparkSession e libera le risorse usate da Spark SQL/DataFrame.
    spark.stop()

    // Chiude lo SparkContext e termina l'applicazione Spark.
    sc.stop()
  }
}
