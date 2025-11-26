
// Definisce il package del progetto
package sparkPractise

// Importa le classi necessarie di Spark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


// Definisce un oggetto singleton Scala chiamato obj_Logs
object obj_Logs {

    // Metodo main: punto di ingresso del programma
    def main(arg:Array[String]):Unit=
    {
        // Crea la configurazione Spark, imposta nome e master
        val conf = new SparkConf().setAppName("TestLog").setMaster("local[*]")
        // Crea lo SparkContext per gestire il job Spark
        val sc = new SparkContext(conf)
        // Imposta il livello di log su Error per meno output
        sc.setLogLevel("Error")
        // Legge il file di testo e crea un RDD, ogni elemento è una riga del file
        val inputRDD = sc.textFile("file:///mnt/nvme_storage/download/Hadoop_2k.log")
        println("The input count is " + inputRDD.count())
        // Stampa ogni riga del file
        inputRDD.foreach(println)
        // Filtra le righe che contengono "WARN"
        val warnRDD = inputRDD.filter(w => w.contains("WARN"))
        warnRDD.foreach(println)
        println("The WARN count is " + warnRDD.count())
        
        // Stampa un'intestazione per i dati di input
        println("************ Input data ************")
        
        // Filtra le righe che contengono "ERROR"
        val ErrorRDD = inputRDD.filter(w => w.contains("ERROR"))
        ErrorRDD.foreach(println)
        println("The ERROR count is " + ErrorRDD.count())
        // Unisce i due RDD di log filtrati
        val unionRDD = warnRDD.union(ErrorRDD)
        //unionRDD.foreach(println)
        println("The error + WARN count is " + unionRDD.count())
        // Stampa alcune righe dell'RDD unito
        //unionRDD.take(5).foreach(println)
        // Restituisce tutte le righe dell'RDD unito al driver
        val x=unionRDD.collect()
        // Stampa la seconda riga dell'RDD unito
        println(x(1))
    }
}

