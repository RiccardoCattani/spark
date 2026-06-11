// Esecuzione:
// sbt "runMain sparkPractise.obj_Logs"
//
// Esempio di analisi log con RDD.
// Il programma legge un file di log Hadoop, conta tutte le righe, estrae quelle
// che contengono WARN ed ERROR, poi unisce i due insiemi. E' utile per vedere
// come filter, union, count, take e collect lavorano su dati testuali.
//
// Attenzione: alcune action qui stampano o raccolgono molti dati. Sono accettabili
// per un file piccolo di esercizio, ma con log grandi e' meglio usare take(n),
// salvare il risultato su file o aggregare i dati prima di portarli sul driver.
package sparkPractise

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object obj_Logs {

    def main(arg:Array[String]):Unit=
    {
        // Configura un'applicazione Spark locale.
        // local[*] usa tutti i core disponibili sulla macchina, quindi permette
        // di simulare un minimo di parallelismo anche senza un cluster.
        val conf = new SparkConf().setAppName("TestLog").setMaster("local[*]")

        // SparkContext consente di creare RDD e avviare job Spark.
        // In questo esempio usiamo l'API RDD per lavorare direttamente sulle righe del file.
        val sc = new SparkContext(conf)

        // Mostra solo i log Spark di errore, cosi' l'output dell'esercizio resta leggibile.
        sc.setLogLevel("Error")

        // Crea un RDD[String] dal file di log: ogni elemento e' una riga.
        // La lettura e' lazy: Spark non accede davvero al file finche' non incontra
        // una action come count(), foreach(), take() o collect().
        val inputRDD = sc.textFile("C:\\repository\\spark\\1.input\\Hadoop_2k.log")
        println("The input count is " + inputRDD.count())

        // foreach esegue una funzione su ogni elemento dell'RDD.
        // Qui stampa tutto il file: utile per vedere l'input, rischioso su file grandi.
        inputRDD.foreach(println)

        // filter conserva solo le righe che soddisfano il predicato.
        // contains("WARN") e' un controllo testuale semplice: trova WARN ovunque nella riga.
        val warnRDD = inputRDD.filter(w => w.contains("WARN"))
        warnRDD.foreach(println)
        println("The WARN count is " + warnRDD.count())

        println("************ Input data ************")

        // Stesso approccio per gli errori. La variabile inizia con maiuscola,
        // ma per convenzione Scala i valori locali di solito partono minuscoli.
        val ErrorRDD = inputRDD.filter(w => w.contains("ERROR"))
        ErrorRDD.foreach(println)
        println("The ERROR count is " + ErrorRDD.count())

        // union concatena logicamente i due RDD senza eliminare duplicati.
        // Se una riga contiene sia WARN sia ERROR, comparira' due volte nel risultato.
        // Per rimuovere duplicati si potrebbe aggiungere distinct().
        val unionRDD = warnRDD.union(ErrorRDD)

        //unionRDD.foreach(println)
        println("The error + WARN count is " + unionRDD.count())

        // take(5) e' piu' sicuro di collect() quando serve solo un campione:
        // porta al driver al massimo cinque righe.
        unionRDD.take(5).foreach(println)

        // collect() materializza l'intero RDD nel driver.
        // Mantenerlo qui e' utile per mostrare l'accesso indicizzato, ma non e'
        // una buona scelta se il risultato puo' essere grande.
        val x=unionRDD.collect()

        // Stampa la seconda riga del risultato raccolto.
        // Se unionRDD avesse meno di due righe, questa istruzione genererebbe errore.
        println(x(1))
    }
}
