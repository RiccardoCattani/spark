package sparkPratica // Definisce il package

import org.apache.spark.sql.SparkSession // Importa la classe SparkSession

object obj_seamless { // Definisce l'oggetto principale
  def main(args: Array[String]): Unit = { // Metodo main, punto di ingresso
    val spark = SparkSession.builder() // Crea una SparkSession
      .appName("Seamless DataFrame Example") // Nome dell'applicazione
      .master("local[*]") // Usa tutti i core locali
      .getOrCreate() // Ottiene o crea la sessione

    spark.sparkContext.setLogLevel("ERROR") // Imposta il livello di log su ERROR

    // Lettura di un file CSV in un DataFrame
    val df = spark.read // Inizia la lettura
      .format("csv") // Specifica il formato CSV
      .option("header", "true") // Indica che il file ha un'intestazione
      .option("delimiter", ",") // Usa la virgola come delimitatore
      .load("/home/riccardo/Documenti/repository/spark/spark/test.csv") // Percorso del file da leggere

    // Mostra lo schema e i primi 5 record
    df.printSchema() // Stampa la struttura delle colonne
    df.show(5) // Mostra i primi 5 record

    // Scrittura del DataFrame in formato Parquet
    df.write // Inizia la scrittura
      .format("parquet") // Specifica il formato Parquet
      .mode("overwrite") // Sovrascrive se esiste già
      .save("percorso/output/parquet") // Percorso di destinazione

    // Scrittura del DataFrame in formato JSON
    df.write // Inizia la scrittura
      .format("json") // Specifica il formato JSON
      .mode("overwrite") // Sovrascrive se esiste già
      .save("percorso/output/json") // Percorso di destinazione

    val inputRDD = spark.sparkContext.textFile("C:/SparkScala/SparkScalaPractise/src/main/scala/sparkPractise/logs/logs.txt") // Esempio di lettura di un file come RDD (non usato nel resto dello script)
  }
}