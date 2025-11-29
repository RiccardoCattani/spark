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
    // Esempio 1: Filtra e stampa solo gli stati dove la lingua è Hindi (con RDD)
    val hindiStatesRDD = inputRDD.filter { line =>
      val fields = line.split(",")
      fields.length >= 3 && fields(2).trim == "Hindi"
    }
    println("\n--- Stati con lingua Hindi (RDD) ---")
    hindiStatesRDD.collect().foreach(println)

    // Esempio 2: Stessa operazione con DataFrame
    import org.apache.spark.sql.SparkSession
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    // Legge il file come DataFrame senza header, con delimitatore virgola
    val df = spark.read
      .option("header", "false")
      .option("inferSchema", "false")
      .option("delimiter", ",")
      .csv("India.txt")
      .toDF("Stato", "Capitale", "Lingua")

    println("\n--- Stati con lingua Hindi (DataFrame) ---")
    df.filter($"Lingua" === "Hindi").show(false)
  }
}
