// sbt run "runMain sparkPratica.obj_PairRDD"
// Questo file contiene un esempio di come creare una Pair RDD a partire da un file di testo e come eseguire alcune operazioni di base su di essa. Assicurati di avere un file di testo chiamato "sales.txt" nella directory specificata, o modifica il percorso del file di conseguenza.

// Definisce il package in cui si trova questo oggetto
package sparkPratica

// Importa le classi necessarie di Spark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// Oggetto principale che contiene il metodo main
object obj_PairRDD {
  // Metodo main, punto di ingresso del programma
  def main(arg: Array[String]): Unit = {
    // Crea una configurazione Spark con nome applicazione e master locale
    val conf = new SparkConf().setAppName("TestLog").setMaster("local[*]")
    // Crea un nuovo SparkContext usando la configurazione
    val sc = new SparkContext(conf)
  
    // Imposta il livello di log su "Error" per ridurre l'output
    sc.setLogLevel("Error")
  
    // Carica il file di testo come RDD da un percorso specificato.
    // Ogni elemento dell'RDD è una riga del file, ad esempio "helmet 20".
    val inputRDD = sc.textFile("C:\\repository\\spark\\1.input\\sales.txt")
  
    // Crea una Pair RDD (chiave, valore) dove la chiave è il primo campo e il valore è il secondo.
    // Una PairRDD ti permette di usare operazioni come keys, values, mapValues, reduceByKey e groupByKey.
    val pairRDD = inputRDD
      .map(_.trim)                         // rimuove spazi iniziali/finali dalla riga
      .filter(line => line.nonEmpty)       // elimina eventuali righe vuote
      .map { line =>
        val arr = line.split("\\s+")   // separa i campi usando uno o più spazi
        if (arr.length < 2) {
          throw new IllegalArgumentException(s"Riga non valida: '$line'")
        }
        // arr(0) è il prodotto, arr(1) è la quantità
        (arr(0), arr(1).toInt)
      }
  
    // Colleziona gli elementi della Pair RDD in un array locale.
    // ATTENZIONE: collect() trasferisce tutti i dati sul driver, quindi va bene solo su dataset piccoli.
    val co = pairRDD.collect()
  
    // Stampa tutte le coppie (prodotto, quantità).
    for (i <- co) {
      println(i)
    }

    // keys: ottiene tutte le chiavi (prodotti) dalla PairRDD.
    // distinct rimuove i duplicati e mostra solo i prodotti unici.
    println("**********Keys**********")
    pairRDD.keys.distinct.collect().foreach(println)

    // values: ottiene solo i valori (quantità) dalla PairRDD.
    println("**********Values**********")
    pairRDD.values.collect().foreach(println)

    // mapValues: trasforma solo i valori mantenendo le stesse chiavi.
    // Esempio: ("helmet", 20) diventa ("helmet", 40).
    println("**********Map Values**********")
    val map_val = pairRDD.mapValues(a => a * 2).collect()
    for (i <- map_val) {
      println(i)
    }

    // reduceByKey: aggrega i valori per chiave usando la funzione specificata.
    // Qui sommiamo le quantità per ogni prodotto.
    // Esempio: ("helmet", 20), ("helmet", 10), ("helmet", 50) diventa ("helmet", 80).
    println("**********ReduceByKey**********")
    val red_by = pairRDD.reduceByKey(_ + _).collect()
    for (i <- red_by) {
      println(i)
    }

    // groupByKey: raggruppa tutti i valori per chiave in una collezione.
    // Risultato: ("helmet", Seq(20, 10, 50)). Usalo solo quando servono i singoli valori,
    // perché è meno efficiente di reduceByKey per aggregazioni di tipo somma.
    println("**********GroupByKey**********")
    val gr = pairRDD.groupByKey().collect()
    for (i <- gr) {
      println(i)
    }

    // Chiude lo SparkContext per rilasciare le risorse
    sc.stop()
  }
}
