package sparkPratica

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object rdd_Oggetto {
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

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("BigBasketJob").setMaster("local[*]")
        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")

        val data = sc.textFile("file:///home/riccardo/datasets/bigbasket_products.csv").cache()
        showRddSample("Input BigBasket CSV letto come RDD di righe", data, limit = 20)

        val fil_category = data.filter(x => x.contains("Beauty")).cache()
        showRddSample("Filtro categoria: righe che contengono Beauty", fil_category)

        val fil_subcategory = fil_category.filter(x => x.contains("Skin Care")).cache()
        showRddSample("Filtro sottocategoria: Beauty + Skin Care", fil_subcategory)

        printSection("Scrittura output")
        println("Partizioni output richieste: 2")
        println("Destinazione: user/cloudera/bigbasket")
        fil_subcategory.coalesce(2).saveAsTextFile("user/cloudera/bigbasket")
        println("Scrittura completata.")

        sc.stop()
    }
}
