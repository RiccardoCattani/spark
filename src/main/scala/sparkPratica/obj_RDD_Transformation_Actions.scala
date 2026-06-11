// Esecuzione:
// sbt "runMain sparkPractise.obj_RDD_Transformation_Actions"
//
// Esempio guidato sulle principali transformation e action degli RDD.
// Il file India.txt viene letto come RDD di righe; poi vengono mostrati map,
// filter, flatMap, distinct, union, intersection, subtract e varie action.
//
// Obiettivo didattico:
// - capire quali operazioni sono lazy transformation
// - capire quali operazioni sono action e quindi avviano davvero il job Spark
// - vedere quando i dati restano distribuiti e quando vengono portati sul driver

package sparkPractise
import java.nio.file.{Files, Path, Paths}
import java.util.Comparator

import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConverters._

object obj_RDD_Transformation_Actions {
  def main(args: Array[String]): Unit = {
    // Configura Spark in modalita' locale.
    // local[*] usa tutti i core della macchina, utile per esercitarsi senza cluster.
    val conf = new SparkConf().setAppName("Logs Analysis").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")

    // textFile crea un RDD[String]: ogni elemento rappresenta una riga del file.
    // Questa e' una transformation lazy: la lettura fisica avviene alla prima action.
    val inputRDD = sc.textFile("C:\\repository\\spark\\1.input\\India.txt")

    // foreach e' una action: esegue println su ogni elemento distribuito.
    // Va bene per file piccoli; su dataset grandi produce troppo output.
    println("************ Input originale ************")
    inputRDD.foreach(println)

    // count e' una action: Spark deve leggere tutte le partizioni per contare le righe.
    println("************ Conteggio righe ************")
    println("Righe totali: " + inputRDD.count())

    // map applica una funzione a ogni elemento e restituisce un nuovo RDD.
    // Qui normalizziamo tutto in maiuscolo per semplificare i filtri successivi.
    println("************ Trasformazione con map: maiuscolo ************")
    val upperRDD = inputRDD.map(x => x.toUpperCase())
    upperRDD.foreach(println)

    // filter conserva solo gli elementi che soddisfano la condizione.
    // Dopo la normalizzazione in maiuscolo, contains("ENGLISH") diventa piu' prevedibile.
    println("************ Filtro con filter: righe con ENGLISH ************")
    val englishRDD = upperRDD.filter(x => x.contains("ENGLISH"))
    englishRDD.foreach(println)
    println("Righe con ENGLISH: " + englishRDD.count())

    println("************ RDD union, intersection, subtract ************")

    // flatMap e' usato quando una riga produce piu' elementi.
    // split(",") divide la riga in campi/parole, map(trim) rimuove spazi iniziali/finali.
    // Il risultato non e' piu' un RDD di righe, ma un RDD di token.
    val englishWordsRDD = englishRDD.flatMap(row => row.split(",")).map(word => word.trim)
    val hindiWordsRDD = upperRDD
      .filter(row => row.contains("HINDI"))
      .flatMap(row => row.split(","))
      .map(word => word.trim)

    // distinct rimuove duplicati tramite shuffle, quindi puo' essere costoso.
    // take(10) limita l'output portato al driver.
    println("Parole da righe ENGLISH")
    englishWordsRDD.distinct().take(10).foreach(println)

    println("Parole da righe HINDI")
    hindiWordsRDD.distinct().take(10).foreach(println)

    // union concatena i due RDD. Da sola non elimina duplicati, per questo qui
    // viene aggiunto distinct() subito dopo.
    println("Union: parole ENGLISH + parole HINDI")
    val unionWordsRDD = englishWordsRDD.union(hindiWordsRDD).distinct()
    unionWordsRDD.take(20).foreach(println)

    // collect porta l'intero RDD sul driver come Array.
    // E' utile per esempi piccoli, ma per dati grandi puo' causare OutOfMemory.
    println("************ Actions su union RDD con collect e for ************")
    val unionRows = unionWordsRDD.collect()
    for (row <- unionRows) {
      println(row)
    }

    // take e' piu' sicuro di collect quando serve solo un campione.
    println("Take: primi 2 elementi della union")
    unionWordsRDD.take(2).foreach(println)

    // intersection restituisce gli elementi presenti in entrambi gli RDD.
    // Anche questa operazione richiede confronto tra partizioni e puo' attivare shuffle.
    println("Intersection: parole presenti sia in ENGLISH sia in HINDI")
    englishWordsRDD.intersection(hindiWordsRDD).foreach(println)

    // subtract restituisce gli elementi presenti nel primo RDD ma non nel secondo.
    // In questo caso mostra parole associate a ENGLISH ed escluse da HINDI.
    println("Subtract: parole ENGLISH escluse quelle HINDI")
    englishWordsRDD.subtract(hindiWordsRDD).distinct().take(20).foreach(println)

    println("************ RDD actions ************")

    // first legge quanto basta per restituire il primo elemento dell'RDD.
    println("Action first: prima riga")
    println(inputRDD.first())

    // take(n) restituisce al driver i primi n elementi.
    println("Action take: prime 5 righe")
    inputRDD.take(5).foreach(println)

    // collect raccoglie tutte le righe sul driver.
    // Qui stampiamo solo le prime cinque, ma l'Array contiene comunque tutto il file.
    println("Action collect: prime 5 righe raccolte sul driver")
    val collectedRows = inputRDD.collect()
    collectedRows.take(5).foreach(println)

    // countByValue conta quante volte compare ogni riga identica.
    // Restituisce una Map sul driver, quindi va usata con attenzione se ci sono molte chiavi.
    println("Action countByValue: prime 5 righe con numero occorrenze")
    inputRDD.countByValue().take(5).foreach {
      case (row, count) => println(row + " -> " + count)
    }

    // takeOrdered usa l'ordinamento naturale per restituire i primi elementi ordinati.
    println("Action takeOrdered: prime 5 righe in ordine alfabetico")
    inputRDD.takeOrdered(5).foreach(println)

    // top e' simile a takeOrdered, ma restituisce gli elementi piu' alti
    // secondo l'ordinamento naturale.
    println("Action top: ultime 5 righe in ordine alfabetico")
    inputRDD.top(5).foreach(println)

    // reduce combina tutti gli elementi dell'RDD usando una funzione associativa.
    // Qui confrontiamo due righe alla volta e manteniamo quella piu' lunga.
    println("Action reduce: riga piu lunga")
    val longestRow = inputRDD.reduce((a, b) => if (a.length >= b.length) a else b)
    println(longestRow)

    // foreachPartition lavora una partizione alla volta.
    // E' utile quando vuoi aprire una connessione o una risorsa una sola volta
    // per partizione, invece che una volta per elemento.
    println("Action foreachPartition: numero righe per partizione")
    inputRDD.foreachPartition(partition => println("Righe nella partizione: " + partition.size))

    // parallelize crea un RDD partendo da una collezione gia' presente nel driver.
    // Serve spesso negli esempi, nei test o per piccoli dataset di supporto.
    println("************ parallelize: create RDD ************")
    val numberRDD = sc.parallelize(List(1, 2, 3, 4, 5))
    numberRDD.foreach(println)

    // reduce sugli interi somma tutti gli elementi.
    // La funzione _ + _ e' associativa, quindi Spark puo' combinarla in parallelo.
    println("************ reduce ************")
    val sum = numberRDD.reduce(_ + _)
    println(sum)

    // Salvataggio locale dell'output in maiuscolo.
    // Qui non usiamo saveAsTextFile di Spark: raccogliamo upperRDD sul driver e
    // scriviamo con le API Java NIO. Va bene per un file piccolo di esercizio.
    val outputPath = "C:\\repository\\spark\\2.output\\india_uppercase.txt"
    deleteIfExists(Paths.get(outputPath))
    Files.createDirectories(Paths.get("C:\\repository\\spark\\2.output"))
    Files.write(Paths.get(outputPath), upperRDD.collect().toSeq.asJava)
    println("Output salvato in: " + outputPath)

    sc.stop()
  }

  // Elimina file o directory esistente prima di riscrivere l'output.
  // La walk viene ordinata al contrario per cancellare prima i file figli
  // e poi la directory padre.
  private def deleteIfExists(path: Path): Unit = {
    if (Files.exists(path)) {
      Files
        .walk(path)
        .sorted(Comparator.reverseOrder())
        .forEach(p => Files.delete(p))
    }
  }
}
