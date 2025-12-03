// Definisce il package in cui si trova questo oggetto
package sparkPratica


// Importa le classi necessarie di Spark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

// Oggetto principale che contiene il metodo main
object obj_PairRDD {
    // Stampa le chiavi distinte della PairRDD
    println("**********Keys**********")
    pairRDD.keys.distinct.collect().foreach(println)
  // Metodo main, punto di ingresso del programma
  def main(arg: Array[String]): Unit = {
    // Crea una configurazione Spark con nome applicazione e master locale
    val conf = new SparkConf().setAppName("TestLog").setMaster("local[*]")
    // Crea un nuovo SparkContext usando la configurazione
    val sc = new SparkContext(conf)
    // Imposta il livello di log su "Error" per ridurre l'output
    sc.setLogLevel("Error")
    // Carica il file di testo come RDD da un percorso specificato
    val inputRDD = sc.textFile("/home/riccardo/Documenti/spark/sales.txt")
    // Crea una Pair RDD (chiave, valore) dove la chiave è il primo campo e valore il secondo
    val pairRDD = inputRDD.map { x =>
      val arr = x.split(" ")
      (arr(1), arr(2).toInt)
    }
    // Colleziona gli elementi della Pair RDD in un array locale
    val co = pairRDD.collect()
    // Stampa ogni elemento della collezione
    for (i <- co) {
      println(i)
    }

    // MapValues: raddoppia ogni valore associato alla chiave
    println("**********Map Values**********")
    val map_val = pairRDD.mapValues(a => a * 2).collect()
    for (i <- map_val) {
      println(i)
    }

    // ReduceByKey: somma le quantità per ogni prodotto
    println("**********ReduceByKey**********")
    val red_by = pairRDD.reduceByKey(_ + _).collect()
    for (i <- red_by) {
      println(i)
    }

    // Raggruppa per chiave e stampa il risultato
    println("**********GroupByKey**********")
    val gr = pairRDD.groupByKey().collect()
    for (i <- gr) {
      println(i)
    }

    // Chiude lo SparkContext per rilasciare le risorse
    sc.stop()
  }
}
