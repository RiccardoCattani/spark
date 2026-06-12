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
  def main(args: Array[String]): Unit = {
    val inputPath = args.headOption.getOrElse("India_pipe.txt")
    val outputPath = args.lift(1).getOrElse("2.output/avro_data")

    val conf = new SparkConf()
      .setAppName("job1")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")

    val spark = SparkSession.builder().getOrCreate()

    val readDf = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", "|")
      .load(inputPath)

    readDf.printSchema()
    readDf.show(10, truncate = false)

    readDf.write
      .format("avro")
      .option("header", "true")
      .mode("overwrite")
      .save(outputPath)

    spark.stop()
    sc.stop()
  }
}
