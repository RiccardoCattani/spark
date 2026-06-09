// sbt run "runMain sparkPractise.obj_WordCount"
// Questo file contiene un esempio di come eseguire un conteggio delle parole (Word Count) usando Spark. Assicurati di avere un file di testo chiamato "words.txt" nella directory specificata, o modifica il percorso del file di conseguenza.

package sparkPractise

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object obj_WordCount {
  def main(arg: Array[String]): Unit = {
    // Crea la configurazione di Spark e imposta l'app in modalità locale
    // con tutti i core disponibili (utile per sviluppo locale)
    val conf = new SparkConf().setAppName("TestLog").setMaster("local[*]")

    // Crea il contesto Spark che permette di creare e trasformare RDD
    val sc   = new SparkContext(conf)

    // Riduce la verbosità dei log mostrando solo gli errori
    sc.setLogLevel("Error")

    // Usa un path locale Linux; aggiorna se necessario
    // `textFile` legge il file e restituisce un RDD di righe (String)
    val inputRDD = sc.textFile("C:\\repository\\spark\\1.input\\words.txt")

    // Trasformazioni per calcolare il conteggio delle parole:
    // 1) flatMap: divide ogni riga in parole (split su spazio) e appiattisce
    // 2) map: trasforma ogni parola in una coppia (parola, 1)
    // 3) reduceByKey: somma i conteggi per ciascuna parola (chiave)
    val word_count = inputRDD
      .flatMap(s => s.split(" "))   // divide le righe in parole
      .map(word => (word, 1))         // crea coppia, ogni parola sarà una chiave con un valore (parola, 1)
      .reduceByKey((x, y) => (x + y)) // somma i conteggi per parola

    // collect() porta i risultati sul driver, ossia prende tutti i dati distribuiti nei worker Spark e li copia nella memoria del driver, cioè il programma principale che ha lanciato Spark. (attenzione con dataset grandi)
    // foreach(println) stampa ogni coppia (parola, conteggio)
    word_count.collect().foreach(println)
  }
}
