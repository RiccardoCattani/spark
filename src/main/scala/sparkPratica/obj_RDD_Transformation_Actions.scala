// Esecuzione:
// sbt "runMain sparkPractise.obj_RDD_Transformation_Actions"

package sparkPractise
import java.nio.file.{Files, Path, Paths}
import java.util.Comparator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.JavaConverters._

object obj_RDD_Transformation_Actions {
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

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Logs Analysis").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")

    val inputRDD = sc.textFile("C:\\repository\\spark\\1.input\\India.txt").cache()
    showRddSample("Input originale India.txt", inputRDD)

    val upperRDD = inputRDD.map(x => x.toUpperCase()).cache()
    showRddSample("Transformation map: righe convertite in maiuscolo", upperRDD)

    val englishRDD = upperRDD.filter(x => x.contains("ENGLISH")).cache()
    showRddSample("Transformation filter: righe che contengono ENGLISH", englishRDD)

    val hindiRDD = upperRDD.filter(row => row.contains("HINDI")).cache()
    showRddSample("Transformation filter: righe che contengono HINDI", hindiRDD)

    val englishWordsRDD = englishRDD.flatMap(row => row.split(",")).map(word => word.trim).cache()
    val hindiWordsRDD = hindiRDD.flatMap(row => row.split(",")).map(word => word.trim).cache()
    showRddSample("flatMap: token estratti dalle righe ENGLISH", englishWordsRDD.distinct())
    showRddSample("flatMap: token estratti dalle righe HINDI", hindiWordsRDD.distinct())

    val unionWordsRDD = englishWordsRDD.union(hindiWordsRDD).distinct().cache()
    showRddSample("Union + distinct: token ENGLISH e HINDI", unionWordsRDD)
    showRddSample("Intersection: token presenti sia in ENGLISH sia in HINDI", englishWordsRDD.intersection(hindiWordsRDD))
    showRddSample("Subtract: token ENGLISH esclusi quelli HINDI", englishWordsRDD.subtract(hindiWordsRDD).distinct())

    printSection("RDD actions su inputRDD")
    println(s"count: ${inputRDD.count()}")
    println(s"first: ${inputRDD.first()}")
    println("take(5):")
    inputRDD.take(5).zipWithIndex.foreach {
      case (row, index) => println(f"${index + 1}%3d | $row")
    }

    printSection("countByValue: prime 5 righe con numero occorrenze")
    inputRDD.countByValue().take(5).foreach {
      case (row, count) => println(s"$row -> $count")
    }

    showRddSample("takeOrdered(5): prime righe in ordine alfabetico", sc.parallelize(inputRDD.takeOrdered(5)))
    showRddSample("top(5): ultime righe in ordine alfabetico", sc.parallelize(inputRDD.top(5)))

    printSection("reduce: riga piu lunga")
    val longestRow = inputRDD.reduce((a, b) => if (a.length >= b.length) a else b)
    println(s"Lunghezza: ${longestRow.length}")
    println(longestRow)

    printSection("foreachPartition: numero righe per partizione")
    val partitionSizes = inputRDD.mapPartitions(partition => Iterator(partition.size)).collect()
    partitionSizes.zipWithIndex.foreach {
      case (size, index) => println(s"Partizione $index -> $size righe")
    }
    println(s"Numero partizioni: ${partitionSizes.length}")

    val numberRDD = sc.parallelize(List(1, 2, 3, 4, 5)).cache()
    showRddSample("parallelize: RDD creato da List(1, 2, 3, 4, 5)", numberRDD)

    printSection("reduce su numberRDD")
    val sum = numberRDD.reduce(_ + _)
    println(s"Somma totale: $sum")

    printSection("Scrittura output locale")
    val outputPath = "C:\\repository\\spark\\2.output\\india_uppercase.txt"
    deleteIfExists(Paths.get(outputPath))
    Files.createDirectories(Paths.get("C:\\repository\\spark\\2.output"))
    Files.write(Paths.get(outputPath), upperRDD.collect().toSeq.asJava)
    println(s"Righe scritte: ${upperRDD.count()}")
    println("Output salvato in: " + outputPath)

    sc.stop()
  }

  private def deleteIfExists(path: Path): Unit = {
    if (Files.exists(path)) {
      Files
        .walk(path)
        .sorted(Comparator.reverseOrder())
        .forEach(p => Files.delete(p))
    }
  }
}
