// Scopo dello script
// ------------------
// Questo script dimostra come convertire un file CSV pipe-delimited in formato
// Avro usando Apache Spark.
//
// Il flusso principale e':
// 1. leggere India_pipe.txt come CSV con header e delimitatore "|";
// 2. caricare i dati in un DataFrame;
// 3. stampare schema e prime righe per controllare la lettura;
// 4. scrivere il DataFrame in formato Avro;
// 5. rileggere l'Avro generato e mostrarne il contenuto.
//
// Esempio prima/dopo
// ------------------
// Prima, CSV pipe-delimited:
// state|capital|language
// Kerala|Thiruvananthapuram|Malayalam
//
// Dopo, DataFrame:
// state  | capital            | language
// Kerala | Thiruvananthapuram | Malayalam
//
// Dopo scrittura Avro:
// 2.output/avro_data/part-*.avro
//
// L'obiettivo e' mostrare che Avro e' un formato binario strutturato, piu'
// adatto del CSV a pipeline Big Data, scambio dati tra sistemi ed evoluzione
// dello schema.
//
// Lo script obj_Avro_File mostra un esempio pratico di utilizzo di Apache Spark per convertire un dataset da formato CSV pipe-delimited a formato Avro. 
// Il programma avvia una sessione Spark in modalità locale, legge il file India_pipe.txt, interpreta la prima riga come intestazione delle colonne e usa il carattere | come delimitatore dei campi.
// Dopo il caricamento, lo script stampa a video lo schema del DataFrame e visualizza le prime 10 righe del dataset, così da permettere una verifica immediata dei dati letti. Infine, scrive il contenuto del DataFrame in formato Avro nella cartella di output configurata, usando la modalità overwrite, quindi sovrascrivendo eventuali dati già presenti nella destinazione.
// In sintesi, lo script serve a dimostrare una semplice pipeline Spark di lettura, controllo e conversione dati da CSV a Avro.
// I vantaggi principali sono questi:
// - Mantiene lo schema
// - Avro conserva nomi e tipi delle colonne. Questo riduce errori quando i dati vengono letti da Spark, Hive, Kafka o altri strumenti.
//
// - È più compatto del CSV
//   Il CSV è testuale e ripete caratteri inutili come delimitatori, intestazioni, separatori. Avro usa un formato binario, quindi occupa meno spazio.
//
// - È più veloce da leggere e scrivere
// - Essendo binario e strutturato, Spark può gestirlo in modo più efficiente rispetto a un semplice file di testo.

// - Gestisce meglio i cambiamenti
// - Se nel tempo aggiungi una colonna o modifichi lo schema, Avro supporta l’evoluzione dello schema. Questo è molto utile nelle pipeline reali.

// - È più adatto ai sistemi distribuiti
// - Avro è molto usato con Spark, Kafka, Hive e Hadoop perché funziona bene nello scambio di dati tra applicazioni diverse.


// Parquet = conserva lo schema, orientato alle colonne, utile per analisi, query e lettura efficiente di poche colonne, questo perché è ottimizzato per query analitiche, consente di leggere solo le colonne necessarie e offre una buona compressione.
// Avro = conserva anche esso lo schema ma orientato alle righe, utile per scambio dati, streaming ed evoluzione dello schema, questo perché conserva i nomi e i tipi delle colonne, è più compatto del CSV e gestisce meglio i cambiamenti di schema.

package sparkPratica

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object obj_Avro_File {
  private def printSection(title: String): Unit = {
    println()
    println("=" * 90)
    println(title)
    println("=" * 90)
  }

  def main(args: Array[String]): Unit = {
    val inputPath = args.headOption.getOrElse("India_pipe.txt")
    val outputPath = args.lift(1).getOrElse("2.output/avro_data")

    printSection("AVVIO - Conversione CSV pipe-delimited in Avro")
    println("Obiettivo: leggere India_pipe.txt come CSV e salvarlo in formato Avro.")
    println(s"Input: $inputPath")
    println(s"Output: $outputPath")

    val conf = new SparkConf()
      .setAppName("job1")
      .setMaster("local[*]")

    printSection("1 - Configurazione Spark")
    println("Creo SparkConf con appName=job1 e master=local[*].")

    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")

    printSection("2 - Creazione SparkSession")
    println("Creo SparkSession per leggere e scrivere DataFrame.")

    val spark = SparkSession.builder().getOrCreate()

    printSection("3 - Lettura CSV con delimitatore pipe")
    println("Spark legge il file come CSV con header=true e delimiter=|.")
    println(s"Path input: $inputPath")
    val readDf = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", "|")
      .load(inputPath)

    println(s"Numero righe lette: ${readDf.count()}")
    println("Schema del DataFrame letto:")
    readDf.printSchema()
    println("Prime 10 righe lette:")
    readDf.show(10, truncate = false)

    printSection("4 - Scrittura in formato Avro")
    println("Spark scrive il DataFrame in formato avro con mode=overwrite.")
    println(s"Path output: $outputPath")
    println("Output atteso: una directory con file part-*.avro e metadati Spark.")
    readDf.write
      .format("avro")
      .option("header", "true")
      .mode("overwrite")
      .save(outputPath)

    printSection("FINE - Job completato")
    println("Il CSV e' stato convertito in una directory Avro.")

    spark.stop()
    sc.stop()
  }
}
