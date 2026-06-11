import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.trim
import org.apache.spark.sql.types._

object ReadingFileWithoutHeader2 {
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
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("depl").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val dml = StructType(Array(
      StructField("state", StringType, true),
      StructField("capital", StringType, true),
      StructField("language", StringType, true),
      StructField("cntry_cd", StringType, true)
    ))

    val df = spark.read
      .option("header", "false")
      .schema(dml)
      .csv("/home/riccardo/Documenti/repository/spark/spark/country")
      .cache()

    showDataFrameDetails("File country letto senza header con schema manuale", df)

    printSection("Riepilogo per codice paese")
    df.groupBy("cntry_cd").count().orderBy("cntry_cd").show(MaxRowsToShow, truncate = false)

    printSection("Scrittura output_one_dir")
    println("Strategia: coalesce(1), formato CSV, mode overwrite")
    println("Destinazione: output_one_dir")
    df.coalesce(1)
      .write
      .mode("overwrite")
      .format("csv")
      .save("file:///home/riccardo/Documenti/repository/spark/spark/output_one_dir")

    val df_clean = df.withColumn("cntry_cd", trim($"cntry_cd")).cache()
    showDataFrameDetails("DataFrame con cntry_cd ripulito tramite trim", df_clean)

    printSection("Scrittura output_by_country")
    println("Partizionamento: cntry_cd")
    df_clean.coalesce(1)
      .write
      .mode("overwrite")
      .format("csv")
      .partitionBy("cntry_cd")
      .save("file:///home/riccardo/Documenti/repository/spark/spark/output_by_country")

    val df_clean_lang = df
      .withColumn("cntry_cd", trim($"cntry_cd"))
      .withColumn("language", trim($"language"))
      .cache()
    showDataFrameDetails("DataFrame con cntry_cd e language ripuliti tramite trim", df_clean_lang)

    printSection("Scrittura output_by_country_language")
    println("Partizionamento: cntry_cd, language")
    df_clean_lang.coalesce(1)
      .write
      .mode("overwrite")
      .format("csv")
      .partitionBy("cntry_cd", "language")
      .save("file:///home/riccardo/Documenti/repository/spark/spark/output_by_country_language")

    spark.stop()
  }
}
