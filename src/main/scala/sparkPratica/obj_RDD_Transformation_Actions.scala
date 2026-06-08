
// sbt "runMain sparkPractise.obj_RDD_Transformation_Actions"
// Questo file contiene un esempio di come creare un RDD a partire da un file di testo e come eseguire alcune operazioni di base su di esso. Puoi aggiungere ulteriori trasformazioni e azioni per esplorare i dati in modo più approfondito.
// Assicurati di avere un file di testo chiamato "India.txt" nella directory specificata, o modifica il percorso del file di conseguenza.

package sparkPractise
import java.nio.file.{Files, Path, Paths}
import java.util.Comparator

import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConverters._

object obj_RDD_Transformation_Actions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Logs Analysis").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")

// Legge il file di testo e crea un RDD, ogni elemento è una riga del file
    val inputRDD = sc.textFile("C:\\repository\\spark\\1.input\\India.txt")

// Stampa alcune righe dell'RDD di input
    println("************ Input originale ************")
    inputRDD.foreach(println)

// Esegue un'azione di conteggio per ottenere il numero totale di righe nell'RDD
    println("************ Conteggio righe ************")
    println("Righe totali: " + inputRDD.count())

// Esegue una trasformazione di mappatura per convertire tutte le righe in maiuscolo
    println("************ Trasformazione con map: maiuscolo ************")
    val upperRDD = inputRDD.map(x => x.toUpperCase())
    upperRDD.foreach(println)

// Esegue una trasformazione di filtro per selezionare solo le righe che contengono "ENGLISH"
    println("************ Filtro con filter: righe con ENGLISH ************")
    val englishRDD = upperRDD.filter(x => x.contains("ENGLISH"))
    englishRDD.foreach(println)
    println("Righe con ENGLISH: " + englishRDD.count())

// Esegue una trasformazione di filtro per selezionare solo le righe che contengono "HINDI"
    println("************ RDD union, intersection, subtract ************")

// Esegue una trasformazione di flatMap per ottenere un RDD di parole da righe ENGLISH e HINDI
    val englishWordsRDD = englishRDD.flatMap(row => row.split(",")).map(word => word.trim)
    val hindiWordsRDD = upperRDD
      .filter(row => row.contains("HINDI"))
      .flatMap(row => row.split(","))
      .map(word => word.trim)

// Stampa alcune parole uniche da righe ENGLISH e HINDI
    println("Parole da righe ENGLISH")
    englishWordsRDD.distinct().take(10).foreach(println)

// Stampa alcune parole uniche da righe HINDI
    println("Parole da righe HINDI")
    hindiWordsRDD.distinct().take(10).foreach(println)

// Esegue una trasformazione di union per combinare le parole uniche da ENGLISH e HINDI
    println("Union: parole ENGLISH + parole HINDI")
    val unionWordsRDD = englishWordsRDD.union(hindiWordsRDD).distinct()
    unionWordsRDD.take(20).foreach(println)

// Esegue alcune azioni su unionWordsRDD
    println("************ Actions su union RDD con collect e for ************")
    val unionRows = unionWordsRDD.collect()
    for (row <- unionRows) {
      println(row)
    }
// Esegue alcune azioni su unionWordsRDD
    println("Take: primi 2 elementi della union")
    unionWordsRDD.take(2).foreach(println)

// Esegue una trasformazione di intersection per trovare parole presenti sia in ENGLISH sia in HINDI
    println("Intersection: parole presenti sia in ENGLISH sia in HINDI")
    englishWordsRDD.intersection(hindiWordsRDD).foreach(println)

// Esegue una trasformazione di subtract per trovare parole presenti in ENGLISH ma non in HINDI
    println("Subtract: parole ENGLISH escluse quelle HINDI")
    englishWordsRDD.subtract(hindiWordsRDD).distinct().take(20).foreach(println)

// Esegue alcune azioni su inputRDD
    println("************ RDD actions ************")

// Esegue l'azione first per ottenere la prima riga dell'RDD
    println("Action first: prima riga")
    println(inputRDD.first())

// Esegue l'azione take per ottenere le prime 5 righe dell'RDD
    println("Action take: prime 5 righe")
    inputRDD.take(5).foreach(println)

// Esegue l'azione collect per raccogliere tutte le righe dell'RDD sul driver e stampare le prime 5
    println("Action collect: prime 5 righe raccolte sul driver")
    val collectedRows = inputRDD.collect()
    collectedRows.take(5).foreach(println)

// Esegue l'azione countByValue per contare le occorrenze di ogni riga e stampare le prime 5
    println("Action countByValue: prime 5 righe con numero occorrenze")
    inputRDD.countByValue().take(5).foreach {
      case (row, count) => println(row + " -> " + count)
    }

// Esegue l'azione takeOrdered per ottenere le prime 5 righe in ordine alfabetico
    println("Action takeOrdered: prime 5 righe in ordine alfabetico")
    inputRDD.takeOrdered(5).foreach(println)


// Esegue l'azione top per ottenere le ultime 5 righe in ordine alfabetico
    println("Action top: ultime 5 righe in ordine alfabetico")
    inputRDD.top(5).foreach(println)

// questo è un esempio di come utilizzare diverse azioni per ottenere informazioni specifiche sull'RDD, come la prima riga, le prime righe, il conteggio delle occorrenze, e l'ordinamento.
    println("Action reduce: riga piu lunga")
    val longestRow = inputRDD.reduce((a, b) => if (a.length >= b.length) a else b)
    println(longestRow)

// questo è un esempio di come utilizzare l'azione reduce per trovare la riga più lunga nell'RDD, confrontando la lunghezza di ogni riga e restituendo quella con la lunghezza maggiore.
    println("Action foreachPartition: numero righe per partizione")
    inputRDD.foreachPartition(partition => println("Righe nella partizione: " + partition.size))

// questo è un esempio di come eseguire un'azione su ogni partizione dell'RDD, in questo caso contando il numero di righe in ogni partizione.
    println("************ parallelize: create RDD ************")
    val numberRDD = sc.parallelize(List(1, 2, 3, 4, 5))
    numberRDD.foreach(println)

// questo è un esempio di come creare un RDD a partire da una collezione in memoria e come eseguire un'azione di riduzione su di esso.
    println("************ reduce ************")
    val sum = numberRDD.reduce(_ + _)
    println(sum)

    val outputPath = "C:\\repository\\spark\\2.output\\india_uppercase.txt"
    deleteIfExists(Paths.get(outputPath))
    Files.createDirectories(Paths.get("C:\\repository\\spark\\2.output"))
    Files.write(Paths.get(outputPath), upperRDD.collect().toSeq.asJava)
    println("Output salvato in: " + outputPath)

    sc.stop()
  }

  private def deleteIfExists(path: Path): Unit = {
    if (Files.exists(path)) {
      Files
        .walk(path)
        .sorted(Comparator.reverseOrder())
        .forEach(p => Files.delete(p))
    }
  }
}
