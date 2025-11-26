package sparkPratica

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object rdd_Oggetto {
    def main(args: Array[String]): Unit = {
        // Crea la configurazione Spark, imposta nome e master
        val conf = new SparkConf().setAppName("BigBasketJob").setMaster("local[*]")
        // Crea lo SparkContext per gestire il job Spark
        val sc = new SparkContext(conf)
        // Imposta il livello di log su ERROR per meno output
        sc.setLogLevel("ERROR")

        // Legge il file CSV da /mnt/nvme_storage/download e crea un RDD, ogni elemento è una riga del file
        val data = sc.textFile("file:///mnt/nvme_storage/download/bigbasket_products.csv")

        // Filtra le righe che contengono "Beauty"
        val fil_category = data.filter(x => x.contains("Beauty"))
        // Filtra ulteriormente le righe che contengono "Skin Care"
        val fil_subcategory = fil_category.filter(x => x.contains("Skin Care"))
        // Stampa le prime 10 righe filtrate
        fil_subcategory.take(10).foreach(println)
        // Salva tutte le righe filtrate in un file di testo (2 partizioni)
        fil_subcategory.coalesce(2).saveAsTextFile("file:///mnt/nvme_storage/download/bigbasket")
    }
}