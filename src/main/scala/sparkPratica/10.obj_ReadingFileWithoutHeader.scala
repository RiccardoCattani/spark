// sbt runMain sparkPratica.obj_ReadingFileWithoutHeader
package sparkPratica

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.trim
import org.apache.spark.sql.types._

// Scopo dello script
// ------------------
// Questo esempio mostra come leggere un file CSV senza intestazione.
//
// Il file countries.txt contiene righe come:
//
// Andhra Pradesh,Amaravati,Telugu,IND
// Alaska,Juneau,English,US
//
// Siccome il file non ha una prima riga con i nomi delle colonne, Spark non sa
// come chiamare i campi. Per questo motivo definiamo manualmente lo schema con
// StructType e StructField.
//
// Lo schema assegna a ogni colonna:
// - un nome;
// - un tipo dato;
// - la possibilita' di contenere valori null.
//
// Esempio prima/dopo
// ------------------
// Prima, riga CSV senza header:
// Alaska,Juneau,English,US
//
// Senza schema Spark creerebbe:
// _c0    | _c1    | _c2     | _c3
// Alaska | Juneau | English | US
//
// Con schema manuale otteniamo:
// state  | capital | language | cntry_cd
// Alaska | Juneau  | English  | US
//
object obj_ReadingFileWithoutHeader {
  private def printSection(title: String): Unit = {
    println()
    println("=" * 90)
    println(title)
    println("=" * 90)
  }

  def main(arg: Array[String]): Unit = {
    printSection("AVVIO - Esempio lettura CSV senza header")
    println("Obiettivo: leggere piu file CSV senza intestazione, applicare uno schema manuale e scrivere output CSV.")
    println("Input: C:\\repository\\spark\\1.input\\country\\countries*")

    // 1. Configurazione Spark.
    //
    // setAppName assegna un nome al job.
    // setMaster("local[*]") esegue Spark in locale usando tutti i core
    // disponibili.
    val conf = new SparkConf()
      .setAppName("depl")
      .setMaster("local[*]")

    // 2. Creazione dello SparkContext.
    //
    // SparkContext rappresenta il contesto base dell'applicazione Spark.
    // setLogLevel("Error") riduce i messaggi di log mostrati a terminale.
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")

    // 3. Creazione della SparkSession.
    //
    // SparkSession e' l'entry point principale per lavorare con DataFrame.
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    // 4. Definizione manuale dello schema.
    //
    // Il file non contiene header, quindi indichiamo noi i nomi delle colonne:
    // state, capital, language e cntry_cd.
    //
    // StringType indica che ogni campo viene letto come stringa.
    // true indica che il campo puo' essere nullo.
    val dml = StructType(Array(
      StructField("state", StringType, true),
      StructField("capital", StringType, true),
      StructField("language", StringType, true),
      StructField("cntry_cd", StringType, true)
    ))

    printSection("1 - Schema manuale definito")
    println("Il file non ha header, quindi assegniamo noi i nomi delle colonne:")
    println("state, capital, language, cntry_cd")
    println("Schema che verra' applicato al CSV:")
    println(dml.treeString)

    // 5. Lettura del CSV senza header.
    //
    // option("header", "false") dice a Spark che la prima riga e' un dato,
    // non una intestazione.
    //
    // schema(dml) applica lo schema definito sopra.
    //
    // Prima:
    // Alaska,Juneau,English,US
    //
    // Dopo:
    // state=Alaska, capital=Juneau, language=English, cntry_cd=US
    val df = spark.read
      .format("csv")
      .option("header", "false")
      .schema(dml)
      .load("C:\\repository\\spark\\1.input\\country\\countries*") // Carica tutti i file che iniziano con countries

    printSection("2 - Lettura file CSV senza header")
    println("Spark legge tutti i file che iniziano con 'countries' nella cartella 1.input\\country.")
    println("L'opzione header=false indica che la prima riga e' un dato, non il nome delle colonne.")

    // 6. Verifica del risultato.
    //
    // printSchema mostra la struttura del DataFrame.
    // show(100, false) mostra fino a 100 righe senza troncare le stringhe.
    // Usiamo 100 per vedere sia i record IND sia i record US: show(false), da
    // solo, mostra soltanto le prime 20 righe.
    println(s"Numero righe lette: ${df.count()}")
    println("Schema del DataFrame letto:")
    df.printSchema()
    println("Dati letti prima della pulizia:")
    df.show(100, false)

    // 7. Pulizia degli spazi finali.
    //
    // Alcune righe dei file di input possono contenere spazi finali, per
    // esempio "US   " invece di "US". Per Spark sono valori diversi.
    //
    // trim rimuove gli spazi iniziali e finali dalle colonne testuali, evitando
    // raggruppamenti e cartelle di partizione duplicate.
    //
    // Prima:
    // cntry_cd = "US   "
    //
    // Dopo:
    // cntry_cd = "US"
    val dfClean = df
      .withColumn("state", trim($"state"))
      .withColumn("capital", trim($"capital"))
      .withColumn("language", trim($"language"))
      .withColumn("cntry_cd", trim($"cntry_cd"))
      .cache()

    printSection("3 - Pulizia spazi iniziali e finali")
    println("Applichiamo trim su state, capital, language e cntry_cd.")
    println("Questo evita valori duplicati come 'US', 'US   ' e 'US      '.")
    println(s"Numero righe dopo pulizia: ${dfClean.count()}")
    println("Dati dopo la pulizia:")
    dfClean.show(100, false)

    // 8. Esempio di interrogazione semplice.
    //
    // Raggruppiamo per codice paese per vedere quanti record appartengono a
    // ogni nazione.
    printSection("4 - Controllo record per codice paese")
    println("Raggruppiamo per cntry_cd per verificare quanti record ci sono per ogni paese.")
    dfClean.groupBy($"cntry_cd")
      .count()
      .orderBy($"cntry_cd")
      .show(false)

    // 9. Scrittura in una sola directory.
    //
    // coalesce(1) riduce il DataFrame a una sola partizione prima della
    // scrittura. In questo modo Spark produce un solo file part-*.csv nella
    // directory di output.
    printSection("5 - Scrittura CSV in una sola directory")
    println("Uso coalesce(1) per produrre un solo file part-*.csv.")
    println("Output: C:\\repository\\spark\\2.output\\output_one_dir")
    dfClean.coalesce(1)
      .write
      .mode("overwrite")
      .format("csv")
      .save("C:\\repository\\spark\\2.output\\output_one_dir")

    // 10. Scrittura partizionata per codice paese.
    //
    // partitionBy("cntry_cd") crea una cartella per ogni valore distinto:
    //
    // output_by_country/cntry_cd=IND
    // output_by_country/cntry_cd=US
    printSection("6 - Scrittura CSV partizionata per paese")
    println("Uso partitionBy(\"cntry_cd\") per creare una cartella per ogni paese.")
    println("Output atteso:")
    println("C:\\repository\\spark\\2.output\\output_by_country\\cntry_cd=IND")
    println("C:\\repository\\spark\\2.output\\output_by_country\\cntry_cd=US")
    dfClean.coalesce(1)
      .write
      .mode("overwrite")
      .format("csv")
      .partitionBy("cntry_cd")
      .save("C:\\repository\\spark\\2.output\\output_by_country")

    // 11. Scrittura partizionata per codice paese e lingua.
    //
    // Spark crea prima le cartelle per cntry_cd e poi, dentro ciascun paese,
    // crea le sottocartelle per language.
    printSection("7 - Scrittura CSV partizionata per paese e lingua")
    println("Uso partitionBy(\"cntry_cd\", \"language\") per creare una doppia partizione.")
    println("Prima Spark divide per paese, poi dentro ogni paese divide per lingua.")
    println("Output: C:\\repository\\spark\\2.output\\output_by_country_language")
    dfClean.coalesce(1)
      .write
      .mode("overwrite")
      .format("csv")
      .partitionBy("cntry_cd", "language")
      .save("C:\\repository\\spark\\2.output\\output_by_country_language")

    printSection("FINE - Job completato")
    println("Sono stati generati tre output: output_one_dir, output_by_country e output_by_country_language.")

    spark.stop()
    sc.stop()
  }
}
