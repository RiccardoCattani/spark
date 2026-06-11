// Esecuzione:
// sbt "runMain sparkPractise.obj_WordCount"

package sparkPractise

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object obj_WordCount {
  private val MaxRowsToShow = 100

  private def printSection(title: String): Unit = {
    println()
    println("=" * 90)
    println(title)
    println("=" * 90)
  }

  private def showRddSample[T](title: String, rdd: RDD[T], limit: Int = MaxRowsToShow): Unit = {
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
    val conf = new SparkConf().setAppName("TestLog").setMaster("local[*]")
    val sc   = new SparkContext(conf)
    sc.setLogLevel("Error")

    val inputRDD = sc.textFile("C:\\repository\\spark\\1.input\\words.txt").cache()
    showRddSample("Input words.txt: righe lette", inputRDD)

    val wordsRDD = inputRDD
      .flatMap(line => line.split("\\s+"))
      .filter(_.nonEmpty)
      .cache()
    showRddSample("Dopo flatMap: parole estratte", wordsRDD)

    val pairsRDD = wordsRDD.map(word => (word, 1)).cache()
    showRddSample("Dopo map: coppie (parola, 1)", pairsRDD)

    val wordCount = pairsRDD.reduceByKey((x, y) => x + y).cache()
    showRddSample("Dopo reduceByKey: conteggio per parola", wordCount)

    val sortedByWord = wordCount.sortByKey()
    showRddSample("Conteggio ordinato alfabeticamente per parola", sortedByWord)

    val sortedByCountDesc = wordCount.sortBy({ case (_, count) => count }, ascending = false)
    showRddSample("Top parole per frequenza decrescente", sortedByCountDesc)

    sc.stop()
  }
}
