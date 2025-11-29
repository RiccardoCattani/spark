

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
    // Applica una trasformazione map per suddividere ogni riga in un array di stringhe
    val pairRDD = inputRDD.map(x => (x.split(" ")))
  }
}
