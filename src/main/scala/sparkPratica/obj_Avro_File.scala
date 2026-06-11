package sparkPratica

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import com.databricks.spark.xml._

object obj_Avro_File {

  def main(args: Array[String]): Unit = {

    // Configurazione Spark per esecuzione locale.
    // setAppName assegna un nome al job; local[*] usa tutti i core disponibili.
    val conf = new SparkConf().setAppName("job1").setMaster("local[*]")

    // SparkContext e' utile per l'API RDD e per controllare impostazioni globali
    // come il livello di log. In questo file la parte strutturata usa SparkSession.
    val sc = new SparkContext(conf)

    // Mostra solo gli errori Spark, riducendo il rumore durante printSchema/show.
    sc.setLogLevel("Error")

    // SparkSession e' necessaria per DataFrame, lettura CSV/XML e funzioni SQL.
    // getOrCreate riusa una sessione gia' esistente oppure ne crea una nuova.
    val spark = SparkSession.builder().getOrCreate()

    // Lettura di un file separato da pipe come DataFrame.
    // Tecnicamente usiamo il reader CSV, ma delimiter="|" indica che i campi
    // non sono separati da virgola. header=true usa la prima riga come nomi colonne.
    val read_df = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", "|")
      .load("/home/riccardo/Documenti/repository/spark/spark/" +
        "India_pipe.txt")

    // printSchema permette di verificare i nomi delle colonne e i tipi inferiti.
    // Senza inferSchema, Spark tende a leggere i valori come stringhe.
    read_df.printSchema()

    // show(10) stampa un campione dei dati sul driver.
    // E' una action: Spark legge i dati necessari per mostrare le prime righe.
    read_df.show(10)

    // Secondo campione piu' piccolo: utile quando vuoi confrontare rapidamente
    // l'output prima e dopo eventuali trasformazioni.
    read_df.show(5)

    // Lettura di un file XML con il datasource spark-xml.
    // rowTag identifica il tag XML che rappresenta una singola riga/record.
    // Se il tag non corrisponde alla struttura reale del file, il DataFrame
    // risultera' vuoto o con schema inatteso.
    val xml_df = spark.read
      .format("xml")
      .option("rowTag", "record")
      .load("/home/riccardo/Documenti/repository/spark/spark/India_xml.xml")

    xml_df.printSchema()
    xml_df.show(5)

    // Scrittura dei primi 100 record del DataFrame CSV in formato XML.
    // rootTag definisce il contenitore esterno del documento XML.
    // rowTag definisce il tag usato per ogni riga del DataFrame.
    // overwrite sostituisce l'output precedente nella directory indicata.
    read_df.limit(100)
      .write
      .format("xml")
      .option("rootTag", "records")
      .option("rowTag", "record")
      .mode("overwrite")
      .save("/home/riccardo/Documenti/repository/spark/spark/output_xml")
  }
}
