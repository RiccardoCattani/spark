package sparkPratica

import com.databricks.spark.xml._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

object obj_Avro_File {
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
    val conf = new SparkConf().setAppName("job1").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")

    val spark = SparkSession.builder().getOrCreate()

    val read_df = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", "|")
      .load("/home/riccardo/Documenti/repository/spark/spark/" +
        "India_pipe.txt")
      .cache()
    showDataFrameDetails("CSV pipe-delimited India_pipe.txt", read_df)

    val xml_df = spark.read
      .format("xml")
      .option("rowTag", "record")
      .load("/home/riccardo/Documenti/repository/spark/spark/India_xml.xml")
      .cache()
    showDataFrameDetails("XML letto con rowTag = record", xml_df)

    printSection("Scrittura XML")
    println("Record sorgente usati: primi 100 record del CSV")
    println("rootTag: records")
    println("rowTag: record")
    println("Modalita': overwrite")
    println("Destinazione: /home/riccardo/Documenti/repository/spark/spark/output_xml")
    read_df.limit(100)
      .write
      .format("xml")
      .option("rootTag", "records")
      .option("rowTag", "record")
      .mode("overwrite")
      .save("/home/riccardo/Documenti/repository/spark/spark/output_xml")
    println("Scrittura XML completata.")

    spark.stop()
    sc.stop()
  }
}
