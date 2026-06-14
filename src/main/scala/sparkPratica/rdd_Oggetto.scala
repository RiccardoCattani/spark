// Scopo dello script
// ------------------
// Questo script dimostra un'elaborazione RDD molto semplice su un file CSV letto
// come righe testuali.
//
// L'obiettivo e' filtrare un dataset di prodotti BigBasket cercando righe che
// contengono prima la categoria Beauty e poi la sottocategoria Skin Care.
// Il risultato filtrato viene scritto in output come file di testo.
//
// Questo esempio mostra un approccio basato su contains, quindi adatto a capire
// le trasformazioni RDD, ma meno strutturato rispetto alla lettura del CSV come
// DataFrame con colonne nominate.
//
// Esempio prima/dopo
// ------------------
// Input CSV come riga testuale:
// 1,Face Cream,Beauty,Skin Care,...
//
// Dopo filter contains("Beauty"):
// rimangono solo le righe che contengono Beauty.
//
// Dopo filter contains("Skin Care"):
// rimangono solo le righe Beauty che contengono anche Skin Care.
//
// Output:
// user/cloudera/bigbasket/part-00000
// user/cloudera/bigbasket/part-00001
//
package sparkPratica

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

// Questo script legge un CSV come RDD di righe e applica filtri testuali.
// L'esempio cerca prodotti BigBasket della categoria Beauty e poi restringe
// ulteriormente ai record che contengono Skin Care.
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
        // Configura Spark in locale e crea lo SparkContext.
        val conf = new SparkConf().setAppName("BigBasketJob").setMaster("local[*]")
        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")

        // Legge il CSV come semplice RDD di stringhe, senza interpretare colonne.
        val data = sc.textFile("file:///home/riccardo/datasets/bigbasket_products.csv").cache()
        showRddSample("Input BigBasket CSV letto come RDD di righe", data, limit = 20)

        // Primo filtro: mantiene solo le righe che contengono Beauty.
        //
        // Prima: tutte le righe del CSV.
        // Dopo: solo righe dove compare la parola Beauty.
        val fil_category = data.filter(x => x.contains("Beauty")).cache()
        showRddSample("Filtro categoria: righe che contengono Beauty", fil_category)

        // Secondo filtro: parte dal risultato precedente e mantiene Skin Care.
        //
        // Prima: righe Beauty.
        // Dopo: righe Beauty + Skin Care.
        val fil_subcategory = fil_category.filter(x => x.contains("Skin Care")).cache()
        showRddSample("Filtro sottocategoria: Beauty + Skin Care", fil_subcategory)

        // Scrive il risultato come file di testo, riducendo l'output a 2 partizioni.
        printSection("Scrittura output")
        println("Partizioni output richieste: 2")
        println("Destinazione: user/cloudera/bigbasket")
        fil_subcategory.coalesce(2).saveAsTextFile("user/cloudera/bigbasket")
        println("Scrittura completata.")

        // Chiude lo SparkContext.
        sc.stop()
    }
}
