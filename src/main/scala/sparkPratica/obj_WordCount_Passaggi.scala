// Esecuzione:
// sbt "runMain sparkPractise.obj_WordCount_Passaggi"

package sparkPractise

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object obj_WordCount_Passaggi {

  // Funzione di supporto usata solo per stampare un RDD con un titolo.
  // collect() porta tutti gli elementi dell'RDD sul driver: va bene qui per debug,
  // ma con dataset grandi e' meglio usare take(n) o salvare su file.
  private def stampaRDD[T](titolo: String, rdd: RDD[T]): Unit = {
    println()
    println(s"========== $titolo ==========")
    rdd.collect().foreach(println)
  }

  def main(arg: Array[String]): Unit = {
    // Configura Spark:
    // - setAppName assegna un nome all'applicazione
    // - local[*] esegue Spark in locale usando tutti i core disponibili
    val conf = new SparkConf()
      .setAppName("WordCountPassaggi")
      .setMaster("local[*]")

    // Crea lo SparkContext, cioe' il punto di ingresso per lavorare con gli RDD.
    val sc = new SparkContext(conf)

    // Mostra solo log di errore, cosi' l'output dei passaggi e' piu' leggibile.
    sc.setLogLevel("Error")

    try {
      // textFile legge il file e crea un RDD[String].
      // Ogni elemento dell'RDD corrisponde a una riga del file words.txt.
      // Esempio:
      // "hello world hello spark"
      // "spark scala world"
      val righeRDD = sc.textFile("words.txt")
      stampaRDD("1. File letto con textFile: una riga per elemento", righeRDD)

      // flatMap trasforma ogni riga in piu' parole.
      // split("\\s+") divide la riga usando uno o piu' spazi/tab come separatore.
      // flatMap poi appiattisce tutto in un unico RDD di parole.
      // Esempio:
      // "hello world hello spark" diventa "hello", "world", "hello", "spark"
      val paroleRDD = righeRDD
        .flatMap(riga => riga.split("\\s+"))

        // filter rimuove eventuali stringhe vuote.
        // Serve soprattutto se il file contiene righe vuote o spazi extra.
        .filter(parola => parola.nonEmpty)
      stampaRDD("2. Dopo flatMap: righe divise in parole", paroleRDD)

      // map trasforma ogni parola in una coppia (parola, 1).
      // Questo prepara i dati per poter sommare i valori per chiave.
      // Esempio:
      // "hello" diventa ("hello", 1)
      val coppieRDD = paroleRDD
        .map(parola => (parola, 1))
      stampaRDD("3. Dopo map: ogni parola diventa (parola, 1)", coppieRDD)

      // reduceByKey raggruppa le coppie con la stessa chiave e somma i valori.
      // La chiave e' la parola, il valore e' il conteggio parziale.
      // Esempio:
      // ("hello", 1), ("hello", 1) diventa ("hello", 2)
      val conteggioRDD = coppieRDD
        .reduceByKey((x, y) => x + y)
      stampaRDD("4. Dopo reduceByKey: somma per parola", conteggioRDD)

      // sortByKey ordina alfabeticamente in base alla chiave della coppia.
      // In questo RDD la chiave e' la parola, quindi il risultato viene ordinato
      // dalla A alla Z.
      // Esempio:
      // ("hello", 2), ("spark", 2), ("scala", 1)
      // diventa:
      // ("hello", 2), ("scala", 1), ("spark", 2)
      val conteggioOrdinatoRDD = conteggioRDD
        .sortByKey()
      stampaRDD("5. Dopo sortByKey: parole in ordine alfabetico", conteggioOrdinatoRDD)
    } finally {
      // Chiude lo SparkContext anche se il programma genera un errore.
      // In questo modo Spark rilascia correttamente le risorse.
      sc.stop()
    }
  }
}
