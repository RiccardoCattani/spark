

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

    // Carica il file di testo come RDD da un percorso specificato
    val inputRDD = sc.textFile("file:///C:/data/sales.txt")
    // Crea una Pair RDD (chiave, valore) dove chiave è il primo campo e valore il secondo
    val pairRDD = inputRDD.map(x => (x.split(" ")(0), x.split(" ")(1)))
    // Colleziona gli elementi della Pair RDD in un array locale
    val co=pairRDD.collect()
    // Stampa ogni elemento della collezione
    for (i <- co) {
      println(i)
    }
  }
}
