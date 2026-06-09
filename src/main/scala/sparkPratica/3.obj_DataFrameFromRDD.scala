/*
  N.B.
  Dataframe = Dataset[Row], quindi un DataFrame e' un Dataset di Row, dove Row e' una struttura che contiene i dati di una riga, con colonne accessibili tramite nomi o indici.
  RDD = Resilient Distributed Dataset, e' la struttura dati fondamentale di Spark, che rappresenta una collezione distribuita di oggetti immutabili. 
        Un RDD non ha schema, e i dati sono trattati come oggetti generici.

  Differenza tra RDD e DataFrame: 
    - un DataFrame ha uno schema, con nomi e tipi di colonne, 
    - mentre un RDD e' una collezione di oggetti senza schema.
  In questo esempio, partiamo da un file di testo, lo leggiamo come RDD, e poi lo trasformiamo in un DataFrame con schema usando una case class. 
  La case class definisce i campi e i tipi dei dati, e quando convertiamo l'RDD in DataFrame, Spark usa i nomi dei campi della case class come nomi delle colonne del DataFrame. 
  Questo ci permette di lavorare con i dati in modo strutturato, applicando filtri e trasformazioni su colonne specifiche, invece di trattare i dati come

  Questo script legge un file di testo senza intestazioni e assegna una struttura ai dati (schema)
  utilizzando una case class. Ogni riga viene suddivisa per delimitatore (virgola) e
  mappata alle colonne: stato, capitale, lingua e paese.
  In questo modo i dati non vengono piu trattati come semplici righe di testo, ma come
  record con colonne nominate. Questo permette di applicare filtri e trasformazioni su
  colonne specifiche, ad esempio filtrare sulla lingua, invece di cercare testo dentro
  l'intera riga.
  Questo è il primo passo per passare da dati grezzi a dati strutturati, che possono essere
  facilmente manipolati con le API di Spark SQL.
  Un passo successivo, mostrato dall'altro script obj_SchemaRDD.scala, è definire esplicitamente lo schema
  usando Row e StructType, e confrontare i due approcci.
  obj_SchemaRDD.scala e' infatti piu completo, confrontando due modi diversi per costruire un
    DataFrame, cioe case class e Row + StructType, e mostra anche filtri con DSL,
    query SQL tramite temporary view e gestione di un output;
  Quindi questo file e' utile per capire il concetto base di "dare una struttura"
  a dati grezzi, mentre obj_SchemaRDD.scala e' utile per vedere piu tecniche Spark SQL.

  Alla fine, i dati strutturati vengono mostrati a console e possono essere filtrati,
  trasformati o salvati in un nuovo file di output.
*/

package sparkPractise

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

case class CountryDml(state: String, capital: String, language: String, country: String)

object obj_DataFrameFromRDD {
  def main(arg: Array[String]): Unit = {
    // Crea SparkContext per leggere il file come RDD.
    val conf = new SparkConf().setAppName("Job1").setMaster("local[*]")
    val sc   = new SparkContext(conf)
    sc.setLogLevel("Error")

    // Crea/recupera la SparkSession per convertire RDD in DataFrame.
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    // Percorso di input relativo alla root del progetto.
    // Esegui lo script da C:\repository\spark con: sbt "runMain sparkPractise.obj_DataFrameFromRDD"
    val inputFile  = sc.textFile("countries.txt")

    // Ogni riga viene divisa usando la virgola, ottenendo un Array[String] con le colonne.
    val inputSplit = inputFile.map(x => x.split(","))

    println("=========== schema rdd ============")

    // La case class assegna un nome e un tipo a ogni campo, creando uno schema leggibile.
    val inputColumns = inputSplit.map(x => CountryDml(x(0), x(1), x(2), x(3)))

    // toDF usa i nomi dei campi della case class come nomi delle colonne del DataFrame.
    val dataframe_schema = inputColumns.toDF()
    dataframe_schema.printSchema()

    // show(false) evita di troncare il contenuto delle colonne durante la stampa.
    dataframe_schema.show(false)
    // val sel_col = dataframe_schema.select("", "")

    spark.stop()
    sc.stop()
  }
}
