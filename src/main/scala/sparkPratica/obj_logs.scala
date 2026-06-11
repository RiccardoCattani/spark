// Esecuzione:
// sbt "runMain sparkPractise.obj_Logs"

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
        val conf = new SparkConf().setAppName("TestLog").setMaster("local[*]")
        val sc = new SparkContext(conf)
        sc.setLogLevel("Error")

        val inputRDD = sc.textFile("C:\\repository\\spark\\1.input\\Hadoop_2k.log").cache()
        showRddSample("Log completo Hadoop_2k.log", inputRDD)

        val warnRDD = inputRDD.filter(w => w.contains("WARN")).cache()
        showRddSample("Filtro log: righe WARN", warnRDD)

        val errorRDD = inputRDD.filter(w => w.contains("ERROR")).cache()
        showRddSample("Filtro log: righe ERROR", errorRDD)

        val unionRDD = warnRDD.union(errorRDD).cache()
        showRddSample("Union WARN + ERROR", unionRDD)

        printSection("Riepilogo conteggi log")
        println(s"Righe totali input: ${inputRDD.count()}")
        println(s"Righe WARN: ${warnRDD.count()}")
        println(s"Righe ERROR: ${errorRDD.count()}")
        println(s"Righe WARN + ERROR: ${unionRDD.count()}")
        println("Nota: union non rimuove duplicati; una riga con WARN e ERROR comparirebbe due volte.")

        sc.stop()
    }
}
