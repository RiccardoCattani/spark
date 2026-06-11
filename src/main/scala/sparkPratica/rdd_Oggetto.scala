package sparkPratica

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object rdd_Oggetto {
    def main(args: Array[String]): Unit = {
        // Configura un job Spark locale dedicato al dataset BigBasket.
        // Il nome dell'app serve per riconoscere il job nei log o nella Spark UI;
        // local[*] usa tutti i core disponibili sulla macchina.
        val conf = new SparkConf().setAppName("BigBasketJob").setMaster("local[*]")

        // SparkContext e' il punto di ingresso dell'API RDD.
        // Gestisce la creazione degli RDD e l'esecuzione delle action.
        val sc = new SparkContext(conf)

        // Riduce il rumore dei log Spark, lasciando visibili solo gli errori.
        sc.setLogLevel("ERROR")

        // Legge il CSV come file di testo: ogni elemento dell'RDD e' una riga completa.
        // Non viene interpretato lo schema CSV; i filtri successivi sono semplici ricerche
        // di sottostringhe dentro la riga. Per parsing robusto di colonne CSV sarebbe meglio
        // usare la DataFrame API con spark.read.option("header", ...).csv(...).
        val data = sc.textFile("file:///home/riccardo/datasets/bigbasket_products.csv")

        // Primo filtro: conserva solo le righe che contengono la categoria "Beauty".
        // contains e' case-sensitive, quindi "beauty" minuscolo non verrebbe trovato.
        val fil_category = data.filter(x => x.contains("Beauty"))

        // Secondo filtro applicato al risultato precedente: restringe il dataset
        // alla sottocategoria "Skin Care".
        val fil_subcategory = fil_category.filter(x => x.contains("Skin Care"))

        // take(10) porta al driver solo un piccolo campione, utile per controllare
        // velocemente che i filtri abbiano selezionato i record attesi.
        fil_subcategory.take(10).foreach(println)

        // coalesce(2) riduce il numero di partizioni di output a due, quindi Spark
        // scrivera' due part file nella directory di destinazione. saveAsTextFile
        // richiede che la directory non esista gia', altrimenti fallisce.
        fil_subcategory.coalesce(2).saveAsTextFile("user/cloudera/bigbasket")
    }
}
