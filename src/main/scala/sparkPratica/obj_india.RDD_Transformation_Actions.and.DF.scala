// Scopo dello script
// ------------------
// Questo script unisce due approcci Spark sullo stesso file India.txt:
// trasformazioni RDD e operazioni DataFrame.
//
// La parte RDD mostra filtri testuali, split delle righe e union tra risultati.
// La parte DataFrame mostra invece come leggere gli stessi dati con colonne
// nominate, filtrare per lingua e calcolare un riepilogo con groupBy.
//
// L'obiettivo e' confrontare il lavoro manuale sugli RDD con il lavoro piu'
// strutturato dei DataFrame.
//
package sparkPractise

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

// Questo script combina esempi RDD e DataFrame sul file India.txt.
// La prima parte usa transformation RDD come filter e union.
// La seconda parte legge lo stesso file come DataFrame per mostrare filtri e groupBy.
object obj_India {
  private val MaxRowsToShow = 100

  private def printSection(title: String): Unit = {
    println()
    println("=" * 90)
    println(title)
    println("=" * 90)
  }

  private def showRddSample(title: String, rdd: RDD[String], limit: Int = 50): Unit = {
    printSection(title)
    val totalRows = rdd.count()
    val rowsToShow = math.min(totalRows, limit).toInt
    println(s"Numero righe: $totalRows")
    println(s"Righe mostrate: $rowsToShow")
    rdd.take(rowsToShow).zipWithIndex.foreach {
      case (row, index) => println(f"${index + 1}%3d | $row")
    }
  }

  private def showDataFrameDetails(title: String, df: DataFrame): Unit = {
    printSection(title)
    val totalRows = df.count()
    val rowsToShow = math.min(totalRows, MaxRowsToShow).toInt
    println(s"Numero colonne: ${df.columns.length}")
    println(s"Colonne: ${df.columns.mkString(", ")}")
    println(s"Numero righe: $totalRows")
    println("Schema:")
    df.printSchema()
    println(s"Dati mostrati: $rowsToShow righe su $totalRows")
    df.show(rowsToShow, truncate = false)
  }

  def main(args: Array[String]): Unit = {
    // Configura Spark in locale e crea lo SparkContext per la parte RDD.
    val conf = new SparkConf().setAppName("India Analysis").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")

    // Legge India.txt come RDD di righe.
    val inputRDD = sc.textFile("India.txt").cache()
    showRddSample("Input RDD completo da India.txt", inputRDD)

    // Filtro RDD robusto: controlla che la riga abbia almeno 3 campi e poi verifica la lingua.
    val hindiStatesRDD = inputRDD.filter { line =>
      val fields = line.split(",")
      fields.length >= 3 && fields(2).trim == "Hindi"
    }
    showRddSample("Filtro RDD: Lingua = Hindi", hindiStatesRDD)

    // Filtri RDD basati su ricerca testuale nella riga completa.
    val filEnglish = inputRDD.filter(_.contains("English"))
    showRddSample("Filtro RDD: record che contengono English", filEnglish)

    val filAndhra = inputRDD.filter(_.contains("Andhra Pradesh"))
    showRddSample("Filtro RDD: record che contengono Andhra Pradesh", filAndhra)

    // union concatena i due RDD filtrati.
    val rddUnion = filEnglish.union(filAndhra)
    showRddSample("Union RDD: English + Andhra Pradesh", rddUnion)

    // Applica un ulteriore filtro sull'RDD ottenuto con union.
    val keyword = "English"
    val filteredUnion = rddUnion.filter(_.contains(keyword))
    showRddSample(s"Union filtrata per keyword = $keyword", filteredUnion)

    // Crea SparkSession per la parte DataFrame.
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    // Legge India.txt come CSV senza header e assegna nomi colonna espliciti.
    val df = spark.read
      .option("header", "false")
      .option("inferSchema", "false")
      .option("delimiter", ",")
      .csv("India.txt")
      .toDF("Stato", "Capitale", "Lingua")
      .cache()

    showDataFrameDetails("DataFrame completo da India.txt", df)

    // Filtro equivalente sul DataFrame: la condizione lavora sulla colonna Lingua.
    showDataFrameDetails("Filtro DataFrame: Lingua = Hindi", df.filter($"Lingua" === "Hindi"))

    // Raggruppa i dati per lingua e conta i record per ogni gruppo.
    printSection("Riepilogo DataFrame per lingua")
    df.groupBy("Lingua").count().orderBy("Lingua").show(MaxRowsToShow, truncate = false)

    // Chiude SparkSession e SparkContext.
    spark.stop()
    sc.stop()
  }
}
