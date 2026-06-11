package sparkPractise

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

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
    val conf = new SparkConf().setAppName("IndiaDF Example").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")

    val inputRDD = sc.textFile("India.txt").cache()
    showRddSample("Input RDD completo da India.txt", inputRDD)

    val hindiStatesRDD = inputRDD.filter { line =>
      val fields = line.split(",")
      fields.length >= 3 && fields(2).trim == "Hindi"
    }
    showRddSample("Filtro RDD: Lingua = Hindi", hindiStatesRDD)

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = spark.read
      .option("header", "false")
      .option("inferSchema", "false")
      .option("delimiter", ",")
      .csv("India.txt")
      .toDF("Stato", "Capitale", "Lingua")
      .cache()

    showDataFrameDetails("DataFrame completo da India.txt", df)
    showDataFrameDetails("Filtro DataFrame: Lingua = Hindi", df.filter($"Lingua" === "Hindi"))

    spark.stop()
    sc.stop()
  }
}
