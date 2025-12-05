package sparkPratica
// Definisce il package in cui si trova questo file (organizzazione del codice).

import org.apache.spark.SparkConf
// Importa la classe per configurare Spark.

import org.apache.spark.sql.SparkSession
// Importa la classe per creare una sessione Spark SQL (necessaria per DataFrame e SQL).

import org.apache.spark.SparkContext
import com.databricks.spark.xml._ // Importa il supporto per XML
// Importa la classe principale per lavorare con RDD e il core di Spark.

object obj_Avro_File {
// Definisce un oggetto Scala chiamato obj_Avro_File (punto di ingresso del programma).

  def main(args: Array[String]): Unit = {
// Metodo main: punto di partenza dell’esecuzione.

    val conf = new SparkConf().setAppName("job1").setMaster("local[*]")
// Crea una configurazione Spark, chiamando l’applicazione “job1” e usando tutti i core locali.

    val sc = new SparkContext(conf)
// Crea lo SparkContext, cioè il contesto di esecuzione Spark, usando la configurazione appena creata.

    sc.setLogLevel("Error")
// Imposta il livello di log su “Error” (mostra solo errori).

    val spark = SparkSession.builder().getOrCreate()
// Crea (o recupera) una SparkSession, necessaria per lavorare con DataFrame e SQL.

    val read_df = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", "|")
      .load("/home/riccardo/Documenti/repository/spark/spark/" +
        "India_pipe.txt")
// Legge il file India_pipe.txt come DataFrame:
// - formato CSV
// - la prima riga è l’intestazione
// - il separatore è |
// - il percorso è quello indicato

    read_df.printSchema()
// Stampa la struttura (schema) delle colonne del DataFrame.

    read_df.show(10)
// Mostra i primi 10 record del DataFrame.

    // Puoi aggiungere qui altre operazioni sul DataFrame
    read_df.show(5)
        // Lettura di un file XML (esempio: India_xml.xml nella stessa cartella)
        val xml_df = spark.read
            .format("xml")
            .option("rowTag", "record") // Cambia 'record' con il tag principale del tuo XML
            .load("/home/riccardo/Documenti/repository/spark/spark/India_xml.xml")
        xml_df.printSchema()
        xml_df.show(5)

        // Scrittura di un DataFrame in formato XML
        // Scrive i primi 100 record del DataFrame CSV in un file XML
        read_df.limit(100)
            .write
            .format("xml")
            .option("rootTag", "records") // Tag radice
            .option("rowTag", "record")   // Tag per ogni riga
            .mode("overwrite")
            .save("/home/riccardo/Documenti/repository/spark/spark/output_xml")
// Mostra i primi 5 record del DataFrame (puoi sostituire o aggiungere altre operazioni qui).


  }
}
