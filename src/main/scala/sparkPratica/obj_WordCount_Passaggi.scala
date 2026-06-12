// Esecuzione:
// sbt "runMain sparkPractise.obj_WordCount_Passaggi"
//
// Scopo dello script
// ------------------
// versione didattica del Word Count. Lo script stampa ogni passaggio intermedio
// per mostrare come un file di testo diventa prima un RDD di parole, poi un RDD
// di coppie (parola, 1), poi un conteggio aggregato per parola.
//
// Rispetto a obj_WordCount, questo file e' pensato per seguire passo passo le
// trasformazioni, quindi stampa ogni RDD intermedio.

package sparkPractise

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object obj_WordCount_Passaggi {
  private val MaxRowsToShow = 100

  private def stampaRDD[T](titolo: String, rdd: RDD[T]): Unit = {
    println()
    println("=" * 90)
    println(titolo)
    println("=" * 90)

    val totalRows = rdd.count()
    val rowsToShow = math.min(totalRows, MaxRowsToShow).toInt
    println(s"Numero elementi: $totalRows")
    println(s"Elementi mostrati: $rowsToShow")
    rdd.take(rowsToShow).zipWithIndex.foreach {
      case (value, index) => println(f"${index + 1}%3d | $value")
    }
  }

  def main(arg: Array[String]): Unit = {
    // Configura Spark in locale.
    val conf = new SparkConf()
      .setAppName("WordCountPassaggi")
      .setMaster("local[*]")

    // SparkContext e' il punto di ingresso per lavorare con gli RDD.
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")

    try {
      // textFile crea un RDD dove ogni elemento corrisponde a una riga del file.
      val righeRDD = sc.textFile("words.txt").cache()
      stampaRDD("1. File letto con textFile: una riga per elemento", righeRDD)

      // flatMap divide le righe in parole e le mette tutte nello stesso RDD.
      // filter elimina eventuali parole vuote.
      val paroleRDD = righeRDD
        .flatMap(riga => riga.split("\\s+"))
        .filter(parola => parola.nonEmpty)
        .cache()
      stampaRDD("2. Dopo flatMap + filter: righe divise in parole valide", paroleRDD)

      // Ogni parola diventa una coppia chiave-valore: la chiave e' la parola,
      // il valore iniziale e' 1.
      val coppieRDD = paroleRDD
        .map(parola => (parola, 1))
        .cache()
      stampaRDD("3. Dopo map: ogni parola diventa (parola, 1)", coppieRDD)

      // reduceByKey somma i valori associati alla stessa parola.
      val conteggioRDD = coppieRDD
        .reduceByKey((x, y) => x + y)
        .cache()
      stampaRDD("4. Dopo reduceByKey: somma per parola", conteggioRDD)

      // Ordina il conteggio alfabeticamente per parola.
      val conteggioOrdinatoRDD = conteggioRDD
        .sortByKey()
      stampaRDD("5. Dopo sortByKey: parole in ordine alfabetico", conteggioOrdinatoRDD)

      // Ordina il conteggio per frequenza decrescente.
      val parolePiuFrequentiRDD = conteggioRDD
        .sortBy({ case (_, count) => count }, ascending = false)
      stampaRDD("6. Ordinamento per frequenza decrescente", parolePiuFrequentiRDD)
    } finally {
      // finally garantisce la chiusura dello SparkContext anche in caso di errore.
      sc.stop()
    }
  }
}
