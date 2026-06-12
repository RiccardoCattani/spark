// Scopo dello script
// ------------------
// Questo script dimostra la differenza pratica tra lavorare con RDD e lavorare
// con DataFrame sugli stessi dati del file India.txt.
//
// Nella prima parte i dati sono letti come RDD di righe testuali: per filtrare
// bisogna dividere manualmente le righe con split e controllare la posizione dei
// campi.
//
// Nella seconda parte lo stesso file viene letto come DataFrame: le colonne
// ricevono nomi espliciti e i filtri diventano piu' leggibili, perche' lavorano
// su colonne come Stato, Capitale e Lingua.
//
package sparkPractise

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

// Questo script confronta due modi di lavorare sugli stessi dati:
// 1. RDD: lettura di righe testuali e filtro manuale tramite split/contains.
// 2. DataFrame: lettura strutturata con colonne nominate e filtri su colonne.
object obj_IndiaDF {
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
    val conf = new SparkConf().setAppName("IndiaDF Example").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")

    // Legge India.txt come RDD di righe.
    val inputRDD = sc.textFile("India.txt").cache()
    showRddSample("Input RDD completo da India.txt", inputRDD)

    // Filtro RDD: divide ogni riga per virgola e controlla il terzo campo, la lingua.
    val hindiStatesRDD = inputRDD.filter { line =>
      val fields = line.split(",")
      fields.length >= 3 && fields(2).trim == "Hindi"
    }
    showRddSample("Filtro RDD: Lingua = Hindi", hindiStatesRDD)

    // Crea la SparkSession per lavorare con DataFrame.
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    // Legge lo stesso file come DataFrame e assegna i nomi alle colonne.
    val df = spark.read
      .option("header", "false")
      .option("inferSchema", "false")
      .option("delimiter", ",")
      .csv("India.txt")
      .toDF("Stato", "Capitale", "Lingua")
      .cache()

    showDataFrameDetails("DataFrame completo da India.txt", df)

    // Filtro DataFrame equivalente al filtro RDD precedente, ma espresso su colonna.
    showDataFrameDetails("Filtro DataFrame: Lingua = Hindi", df.filter($"Lingua" === "Hindi"))

    // Chiude SparkSession e SparkContext.
    spark.stop()
    sc.stop()
  }
}
