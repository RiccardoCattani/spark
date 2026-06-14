// Esecuzione:
// sbt "runMain sparkPractise.obj_WordCount"
//
// Scopo dello script
// ------------------
// questo script mostra il classico esempio Word Count con Spark RDD.
// Il file words.txt viene letto come RDD di righe, poi ogni riga viene divisa
// in parole, ogni parola viene trasformata in una coppia (parola, 1) e infine
// reduceByKey somma i valori per ottenere il numero di occorrenze di ogni parola.
//
// Serve a dimostrare il flusso base:
// testo -> parole -> coppie chiave-valore -> aggregazione per chiave.
//
// Esempio prima/dopo
// ------------------
// Input:
// ciao spark
// ciao scala
//
// Dopo flatMap:
// ciao, spark, ciao, scala
//
// Dopo map:
// (ciao,1), (spark,1), (ciao,1), (scala,1)
//
// Dopo reduceByKey:
// (ciao,2), (spark,1), (scala,1)

package sparkPractise

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object obj_WordCount {
  private val MaxRowsToShow = 100

  private def printSection(title: String): Unit = {
    println()
    println("=" * 90)
    println(title)
    println("=" * 90)
  }

  private def showRddSample[T](title: String, rdd: RDD[T], limit: Int = MaxRowsToShow): Unit = {
    printSection(title)
    val totalRows = rdd.count()
    val rowsToShow = math.min(totalRows, limit).toInt
    println(s"Numero elementi: $totalRows")
    println(s"Elementi mostrati: $rowsToShow")
    rdd.take(rowsToShow).zipWithIndex.foreach {
      case (value, index) => println(f"${index + 1}%3d | $value")
    }
  }

  def main(arg: Array[String]): Unit = {
    printSection("AVVIO - Word Count con Spark RDD")
    println("Obiettivo: leggere words.txt, estrarre parole e contare le occorrenze.")
    println("Input: C:\\repository\\spark\\1.input\\words.txt")

    // Configura Spark in modalita' locale usando tutti i core disponibili.
    val conf = new SparkConf().setAppName("TestLog").setMaster("local[*]")
    val sc   = new SparkContext(conf)
    sc.setLogLevel("Error")

    // Legge il file di testo: ogni riga del file diventa un elemento dell'RDD.
    printSection("1 - Lettura file di testo")
    println("SparkContext legge words.txt: ogni riga diventa un elemento dell'RDD.")
    val inputRDD = sc.textFile("C:\\repository\\spark\\1.input\\words.txt").cache()
    showRddSample("Input words.txt: righe lette", inputRDD)

    // flatMap divide ogni riga in parole e produce un unico RDD di parole.
    // filter rimuove eventuali stringhe vuote.
    //
    // Prima:
    // "ciao spark"
    //
    // Dopo:
    // "ciao", "spark"
    printSection("2 - Estrazione parole con flatMap")
    println("flatMap divide ogni riga in parole; filter rimuove eventuali stringhe vuote.")
    val wordsRDD = inputRDD
      .flatMap(line => line.split("\\s+"))
      .filter(_.nonEmpty)
      .cache()
    showRddSample("Dopo flatMap: parole estratte", wordsRDD)

    // Ogni parola viene trasformata in una coppia chiave-valore: (parola, 1).
    printSection("3 - Creazione coppie chiave-valore")
    println("Ogni parola diventa una coppia (parola, 1).")
    val pairsRDD = wordsRDD.map(word => (word, 1)).cache()
    showRddSample("Dopo map: coppie (parola, 1)", pairsRDD)

    // reduceByKey raggruppa per parola e somma tutti gli 1 associati alla stessa chiave.
    //
    // Prima:
    // (ciao,1), (ciao,1)
    //
    // Dopo:
    // (ciao,2)
    printSection("4 - Conteggio con reduceByKey")
    println("reduceByKey raggruppa per parola e somma i valori associati alla stessa chiave.")
    val wordCount = pairsRDD.reduceByKey((x, y) => x + y).cache()
    showRddSample("Dopo reduceByKey: conteggio per parola", wordCount)

    // Ordina il risultato per chiave, quindi alfabeticamente per parola.
    printSection("5 - Ordinamento alfabetico")
    println("sortByKey ordina il risultato per parola.")
    val sortedByWord = wordCount.sortByKey()
    showRddSample("Conteggio ordinato alfabeticamente per parola", sortedByWord)

    // Ordina il risultato per valore, quindi per frequenza decrescente.
    printSection("6 - Ordinamento per frequenza")
    println("sortBy ordina le parole in base al conteggio, in ordine decrescente.")
    val sortedByCountDesc = wordCount.sortBy({ case (_, count) => count }, ascending = false)
    showRddSample("Top parole per frequenza decrescente", sortedByCountDesc)

    // Chiude lo SparkContext e libera le risorse.
    printSection("FINE - Job completato")
    println("Il file e' stato trasformato in conteggio parole tramite RDD.")
    sc.stop()
  }
}
