// Esecuzione:
// sbt "runMain sparkPractise.obj_Logs"
//
// Scopo dello script
// ------------------
// questo script legge un file di log Hadoop come RDD di righe e mostra semplici
// trasformazioni di filtro: righe WARN, righe ERROR e unione dei due risultati.
//
// L'obiettivo e' mostrare come usare filter per estrarre eventi specifici da un
// log e union per combinare piu insiemi di righe. Alla fine vengono stampati i
// conteggi per confrontare input, WARN, ERROR e risultato combinato.

package sparkPractise

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object obj_Logs {
    private val MaxRowsToShow = 100

    private def printSection(title: String): Unit = {
        println()
        println("=" * 90)
        println(title)
        println("=" * 90)
    }

    private def showRddSample(title: String, rdd: RDD[String], limit: Int = MaxRowsToShow): Unit = {
        printSection(title)
        val totalRows = rdd.count()
        val rowsToShow = math.min(totalRows, limit).toInt
        println(s"Numero righe: $totalRows")
        println(s"Righe mostrate: $rowsToShow")
        rdd.take(rowsToShow).zipWithIndex.foreach {
            case (row, index) => println(f"${index + 1}%3d | $row")
        }
    }

    def main(arg:Array[String]):Unit=
    {
        // Configura Spark in locale e crea lo SparkContext per lavorare con RDD.
        val conf = new SparkConf().setAppName("TestLog").setMaster("local[*]")
        val sc = new SparkContext(conf)
        sc.setLogLevel("Error")

        // Legge il file di log: ogni riga del file diventa un elemento dell'RDD.
        val inputRDD = sc.textFile("C:\\repository\\spark\\1.input\\Hadoop_2k.log").cache()
        showRddSample("Log completo Hadoop_2k.log", inputRDD)

        // Filtra solo le righe che contengono la parola WARN.
        val warnRDD = inputRDD.filter(w => w.contains("WARN")).cache()
        showRddSample("Filtro log: righe WARN", warnRDD)

        // Filtra solo le righe che contengono la parola ERROR.
        val errorRDD = inputRDD.filter(w => w.contains("ERROR")).cache()
        showRddSample("Filtro log: righe ERROR", errorRDD)

        // Unisce i due RDD filtrati. union concatena i risultati e non elimina duplicati.
        val unionRDD = warnRDD.union(errorRDD).cache()
        showRddSample("Union WARN + ERROR", unionRDD)

        // Stampa i conteggi finali per confrontare input e output dei filtri.
        printSection("Riepilogo conteggi log")
        println(s"Righe totali input: ${inputRDD.count()}")
        println(s"Righe WARN: ${warnRDD.count()}")
        println(s"Righe ERROR: ${errorRDD.count()}")
        println(s"Righe WARN + ERROR: ${unionRDD.count()}")
        println("Nota: union non rimuove duplicati; una riga con WARN e ERROR comparirebbe due volte.")

        // Chiude lo SparkContext.
        sc.stop()
    }
}
