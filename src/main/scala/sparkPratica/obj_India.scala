// Definisce il package del progetto
package sparkPractise

// Importa le classi principali di Spark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

// Definisce un oggetto singleton Scala chiamato obj_India
object obj_India {
  // Metodo main: punto di ingresso del programma
  def main(args: Array[String]): Unit = {
    // Crea la configurazione Spark, imposta nome e master
    val conf = new SparkConf().setAppName("India Analysis").setMaster("local[*]")
    // Crea lo SparkContext per gestire il job Spark
    val sc = new SparkContext(conf)
    // Imposta il livello di log su Error per meno output
    sc.setLogLevel("Error")

    // Legge il file di testo e crea un RDD, ogni elemento è una riga del file
    val inputRDD = sc.textFile("India.txt")
    // Stampa ogni riga del file
    inputRDD.foreach(println)
  }
}
