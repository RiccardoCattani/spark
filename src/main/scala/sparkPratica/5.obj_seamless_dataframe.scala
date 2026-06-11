import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

object obj_SeamlessDataframe {
  private val MaxRowsToShow = 100

  private def printSection(title: String): Unit = {
    println()
    println("=" * 90)
    println(title)
    println("=" * 90)
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

    if (totalRows > MaxRowsToShow) {
      println(s"Nota: il DataFrame contiene $totalRows righe; ne mostro solo $MaxRowsToShow per non riempire troppo la console.")
    }
  }

  private def printWriteDetails(format: String, mode: String, destination: String, extra: String = ""): Unit = {
    printSection(s"Scrittura formato $format")
    println(s"Formato: $format")
    println(s"Modalita': $mode")
    println(s"Destinazione: $destination")
    if (extra.nonEmpty) {
      println(extra)
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Job1")
      .setMaster("local[*]")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val read_csv_df = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load("file:///C:/data/train.csv")
      .cache()
    showDataFrameDetails("Lettura CSV train.csv", read_csv_df)

    val read_pipe_df = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", "|")
      .load("file:///C:/data/india_pipe.txt")
      .cache()
    showDataFrameDetails("Lettura CSV pipe-delimited india_pipe.txt", read_pipe_df)

    printWriteDetails(
      format = "csv",
      mode = "overwrite",
      destination = "file:///C:/data/output/file_india_csv",
      extra = "Header: true | Delimiter output: ~"
    )
    read_pipe_df.write
      .format("csv")
      .option("header", "true")
      .option("delimiter", "~")
      .mode("overwrite")
      .save("file:///C:/data/output/file_india_csv")
    println("Scrittura CSV completata.")

    printWriteDetails(
      format = "orc",
      mode = "overwrite",
      destination = "file:///C:/data/output/file_india_orc"
    )
    read_pipe_df.write
      .format("orc")
      .mode("overwrite")
      .save("file:///C:/data/output/file_india_orc")
    println("Scrittura ORC completata.")

    printWriteDetails(
      format = "parquet",
      mode = "overwrite",
      destination = "file:///C:/data/output/file_india_parquet"
    )
    read_pipe_df.write
      .format("parquet")
      .mode("overwrite")
      .save("file:///C:/data/output/file_india_parquet")
    println("Scrittura Parquet completata.")

    printWriteDetails(
      format = "json",
      mode = "overwrite",
      destination = "file:///C:/data/output/file_india_json"
    )
    read_pipe_df.write
      .format("json")
      .mode("overwrite")
      .save("file:///C:/data/output/file_india_json")
    println("Scrittura JSON completata.")

    val orc_df = spark.read
      .format("orc")
      .load("file:///C:/data/output/file_india_orc")
    showDataFrameDetails("Rilettura output ORC", orc_df)

    val parquet_df = spark.read
      .format("parquet")
      .load("file:///C:/data/output/file_india_parquet")
    showDataFrameDetails("Rilettura output Parquet", parquet_df)

    val json_df = spark.read
      .format("json")
      .load("file:///C:/data/output/file_india_json")
    showDataFrameDetails("Rilettura output JSON", json_df)

    spark.stop()
  }
}
