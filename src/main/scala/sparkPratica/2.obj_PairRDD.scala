// Esecuzione:
// sbt "runMain sparkPratica.obj_PairRDD"
//
// Scopo dello script
// ------------------
// questo script mostra le operazioni principali sui PairRDD, cioe' RDD composti
// da coppie chiave-valore. L'esempio legge vendite prodotto/quantita e applica
// keys, values, mapValues, reduceByKey e groupByKey.
//
// L'obiettivo e' capire come Spark lavora quando ogni record ha una chiave e un
// valore, per esempio prodotto -> quantita. Questo modello e' molto comune per
// aggregazioni, conteggi e riepiloghi per categoria.

package sparkPratica

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object obj_PairRDD {
  private val MaxRowsToShow = 100

  private def printSection(title: String): Unit = {
    println()
    println("=" * 90)
    println(title)
    println("=" * 90)
  }

  private def showPairRdd[K, V](title: String, rdd: RDD[(K, V)], limit: Int = MaxRowsToShow): Unit = {
    printSection(title)
    val totalRows = rdd.count()
    val rowsToShow = math.min(totalRows, limit).toInt
    println(s"Numero coppie: $totalRows")
    println(s"Coppie mostrate: $rowsToShow")
    rdd.take(rowsToShow).zipWithIndex.foreach {
      case ((key, value), index) => println(f"${index + 1}%3d | chiave=$key | valore=$value")
    }
  }

  private def showRdd[T](title: String, rdd: RDD[T], limit: Int = MaxRowsToShow): Unit = {
    printSection(title)
    val totalRows = rdd.count()
    val rowsToShow = math.min(totalRows, limit).toInt
    println(s"Numero elementi: $totalRows")
    println(s"Elementi mostrati: $rowsToShow")
    rdd.take(rowsToShow).zipWithIndex.foreach {
      case (value, index) => println(f"${index + 1}%3d | $value")
    }
  }

  def main(arg: Array[String]): Unit = {
    // Configura Spark in locale e crea lo SparkContext.
    val conf = new SparkConf().setAppName("TestLog").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")

    // Legge sales.txt come RDD di righe testuali.
    val inputRDD = sc.textFile("C:\\repository\\spark\\1.input\\sales.txt").cache()
    showRdd("Input sales.txt", inputRDD)

    // Converte ogni riga in una coppia (prodotto, quantita).
    // trim e filter eliminano spazi esterni e righe vuote.
    val pairRDD = inputRDD
      .map(_.trim)
      .filter(line => line.nonEmpty)
      .map { line =>
        val arr = line.split("\\s+")
        if (arr.length < 2) {
          throw new IllegalArgumentException(s"Riga non valida: '$line'")
        }
        (arr(0), arr(1).toInt)
      }
      .cache()

    showPairRdd("PairRDD iniziale: prodotto -> quantita", pairRDD)

    // keys estrae solo le chiavi; distinct rimuove prodotti duplicati.
    showRdd("Chiavi distinte: prodotti", pairRDD.keys.distinct())

    // values estrae solo i valori numerici.
    showRdd("Valori: quantita originali", pairRDD.values)

    // mapValues modifica solo il valore, lasciando invariata la chiave.
    showPairRdd("mapValues: quantita raddoppiata mantenendo la chiave", pairRDD.mapValues(a => a * 2))

    // reduceByKey aggrega le quantita per prodotto sommando i valori.
    showPairRdd("reduceByKey: somma quantita per prodotto", pairRDD.reduceByKey(_ + _))

    printSection("groupByKey: valori originali raggruppati per prodotto")
    // groupByKey raccoglie tutti i valori associati alla stessa chiave.
    val grouped = pairRDD.groupByKey().cache()
    println(s"Prodotti distinti: ${grouped.count()}")
    grouped.take(MaxRowsToShow).zipWithIndex.foreach {
      case ((product, values), index) =>
        val list = values.toList
        println(f"${index + 1}%3d | prodotto=$product | valori=${list.mkString("[", ", ", "]")} | occorrenze=${list.size}")
    }

    // Chiude lo SparkContext.
    sc.stop()
  }
}
