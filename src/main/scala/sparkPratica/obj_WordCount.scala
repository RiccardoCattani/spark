// Esecuzione:
// sbt "runMain sparkPractise.obj_WordCount"
//
// Esempio didattico di Word Count con RDD.
// Il programma legge un file di testo, spezza le righe in parole, associa ogni parola
// al valore 1 e poi somma i valori con la stessa chiave. Questa e' una delle pipeline
// Spark piu' classiche per capire la differenza tra:
// - transformation: costruiscono un nuovo RDD in modo lazy, senza eseguire subito il job
// - action: avviano davvero il calcolo e riportano/stampano un risultato
//
// Nota sui percorsi: il file di input e' indicato con un path locale Windows.
// Se il progetto viene eseguito su un'altra macchina, aggiorna il percorso.

package sparkPractise

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object obj_WordCount {
  def main(arg: Array[String]): Unit = {
    // SparkConf contiene le impostazioni base dell'applicazione Spark.
    // setAppName assegna un nome visibile nei log e nella Spark UI.
    // local[*] esegue Spark in locale usando tutti i core disponibili: e' comodo
    // per esercizi e test, mentre in cluster il master verrebbe gestito da YARN,
    // Kubernetes, standalone cluster, ecc.
    val conf = new SparkConf().setAppName("TestLog").setMaster("local[*]")

    // SparkContext e' il punto di ingresso della vecchia API RDD.
    // Da qui partono lettura dei file, creazione degli RDD e invio dei job Spark.
    val sc   = new SparkContext(conf)

    // Riduce la verbosita' dei log Spark. In esercizi didattici aiuta a vedere
    // chiaramente le println senza essere sommersi dai messaggi INFO.
    sc.setLogLevel("Error")

    // textFile legge il file come RDD[String].
    // Ogni elemento dell'RDD corrisponde a una riga del file, non a una parola.
    // Il contenuto non viene letto immediatamente: Spark prepara solo il piano
    // di esecuzione; la lettura reale avverra' alla prima action, per esempio count().
    val inputRDD = sc.textFile("C:\\repository\\spark\\1.input\\words.txt")

    // Prima trasformazione della pipeline:
    // - split(" ") divide ogni riga usando lo spazio come separatore
    // - flatMap appiattisce il risultato, quindi da RDD[Array[String]] otteniamo RDD[String]
    // - cache() chiede a Spark di mantenere wordsRDD in memoria dopo il primo calcolo
    //
    // cache ha senso perche' wordsRDD viene usato piu' volte: count(), take() e map().
    // Senza cache, Spark potrebbe ricalcolare la stessa trasformazione ogni volta.
    val wordsRDD = inputRDD
      .flatMap(line => line.split(" ")).cache()

    println("=== Dopo flatMap ===")
    println(s"Conteggio totale parole (count): ${wordsRDD.count()}")
    println("Esempio parole:")
    wordsRDD.take(20).foreach(word => println(s"  '$word'"))

    // map prepara i dati per una aggregazione key/value.
    // Ogni parola diventa una coppia (parola, 1): la parola e' la chiave,
    // il numero 1 e' il contributo di una singola occorrenza.
    val pairsRDD = wordsRDD
      .map(word => (word, 1))

    println("=== Dopo map ===")
    println(s"Conteggio coppie parola->1: ${pairsRDD.count()}")
    println("Esempio coppie:")
    pairsRDD.take(20).foreach(pair => println(s"  $pair"))

    // reduceByKey raggruppa le coppie con la stessa chiave e combina i valori.
    // A differenza di groupByKey, puo' fare aggregazioni parziali lato mapper,
    // riducendo i dati da trasferire durante lo shuffle.
    val word_count = pairsRDD.reduceByKey((x, y) => x + y)

    println("=== Dopo reduceByKey ===")
    println(s"Conteggio parole distinte: ${word_count.count()}")

    // collect() porta tutti i risultati sul driver, cioe' nel processo principale.
    // Va bene in un esempio piccolo; con dataset grandi puo' saturare la memoria.
    // In produzione e' piu' prudente usare take(n), saveAsTextFile o scrivere su storage.
    word_count.collect().foreach(println)
  }
}
