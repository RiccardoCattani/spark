// Esecuzione:
// sbt "runMain sparkPractise.obj_WordCount_Passaggi"

package sparkPractise

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object obj_WordCount_Passaggi {
  private val MaxRowsToShow = 100

  private def stampaRDD[T](titolo: String, rdd: RDD[T]): Unit = {
    println()
    println("=" * 90)
    println(titolo)
    println("=" * 90)

    val totalRows = rdd.count()
    val rowsToShow = math.min(totalRows, MaxRowsToShow).toInt
    println(s"Numero elementi: $totalRows")
    println(s"Elementi mostrati: $rowsToShow")
    rdd.take(rowsToShow).zipWithIndex.foreach {
      case (value, index) => println(f"${index + 1}%3d | $value")
    }
  }

  def main(arg: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("WordCountPassaggi")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")

    try {
      val righeRDD = sc.textFile("words.txt").cache()
      stampaRDD("1. File letto con textFile: una riga per elemento", righeRDD)

      val paroleRDD = righeRDD
        .flatMap(riga => riga.split("\\s+"))
        .filter(parola => parola.nonEmpty)
        .cache()
      stampaRDD("2. Dopo flatMap + filter: righe divise in parole valide", paroleRDD)

      val coppieRDD = paroleRDD
        .map(parola => (parola, 1))
        .cache()
      stampaRDD("3. Dopo map: ogni parola diventa (parola, 1)", coppieRDD)

      val conteggioRDD = coppieRDD
        .reduceByKey((x, y) => x + y)
        .cache()
      stampaRDD("4. Dopo reduceByKey: somma per parola", conteggioRDD)

      val conteggioOrdinatoRDD = conteggioRDD
        .sortByKey()
      stampaRDD("5. Dopo sortByKey: parole in ordine alfabetico", conteggioOrdinatoRDD)

      val parolePiuFrequentiRDD = conteggioRDD
        .sortBy({ case (_, count) => count }, ascending = false)
      stampaRDD("6. Ordinamento per frequenza decrescente", parolePiuFrequentiRDD)
    } finally {
      sc.stop()
    }
  }
}
