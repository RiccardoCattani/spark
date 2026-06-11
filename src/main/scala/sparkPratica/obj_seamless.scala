package sparkPratica

import org.apache.spark.sql.SparkSession

object obj_seamless {
  def main(args: Array[String]): Unit = {
    // SparkSession e' l'entry point moderno per lavorare con DataFrame, Dataset e SQL.
    // Rispetto a SparkContext, espone API piu' comode per file strutturati come CSV,
    // JSON e Parquet. local[*] esegue tutto in locale usando i core disponibili.
    val spark = SparkSession.builder()
      .appName("Seamless DataFrame Example")
      .master("local[*]")
      .getOrCreate()

    // Riduce la quantita' di log prodotti da Spark, cosi' schema e dati mostrati
    // con printSchema/show restano facili da leggere.
    spark.sparkContext.setLogLevel("ERROR")

    // Lettura di un CSV in un DataFrame.
    // format("csv") seleziona il datasource CSV.
    // header=true dice a Spark di usare la prima riga come nomi delle colonne.
    // delimiter="," specifica il separatore tra i campi.
    // Senza inferSchema, Spark legge normalmente le colonne come String.
    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load("/home/riccardo/Documenti/repository/spark/spark/test.csv")

    // printSchema mostra nomi e tipi delle colonne.
    // show(5) e' una action: legge i dati necessari e stampa solo le prime cinque righe.
    df.printSchema()
    df.show(5)

    // Scrittura in formato Parquet.
    // Parquet e' colonnare: e' adatto ad analisi, query selettive su poche colonne
    // e integrazione con motori come Spark, Hive e Impala.
    // mode("overwrite") elimina il risultato precedente nella stessa directory.
    df.write
      .format("parquet")
      .mode("overwrite")
      .save("percorso/output/parquet")

    // Scrittura in formato JSON.
    // Spark salva una directory con uno o piu' part file, non un singolo file JSON.
    // Il numero di file dipende dalle partizioni del DataFrame.
    df.write
      .format("json")
      .mode("overwrite")
      .save("percorso/output/json")

    // Esempio di lettura dello stesso contenuto tramite API RDD.
    // In questo script l'RDD non viene usato dopo la creazione: serve solo a mostrare
    // che dalla SparkSession si puo' ancora accedere allo SparkContext.
    val inputRDD = spark.sparkContext.textFile("C:/SparkScala/SparkScalaPractise/src/main/scala/sparkPractise/logs/logs.txt")
  }
}
