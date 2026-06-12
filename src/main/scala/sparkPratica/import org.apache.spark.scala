import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

// Questo script legge un file CSV senza intestazione.
// Poiche' il file non contiene i nomi delle colonne, lo schema viene definito
// manualmente con StructType e applicato durante la lettura.
object ReadingFileWithoutHeader {
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
    // Configura Spark in locale e crea la SparkSession.
    val conf = new SparkConf().setAppName("depl").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    // Definisce manualmente lo schema del file: nome colonna, tipo e nullable.
    val dml = StructType(Array(
      StructField("state", StringType, true),
      StructField("capital", StringType, true),
      StructField("language", StringType, true),
      StructField("cntry_cd", StringType, true)
    ))

    // Legge il CSV senza header usando lo schema definito sopra.
    val df = spark.read
      .option("header", "false")
      .schema(dml)
      .csv("/home/riccardo/Documenti/repository/spark/sparkfile.csv")
      .cache()

    showDataFrameDetails("File senza header letto con schema manuale", df)

    // Raggruppa per codice paese e conta quanti record ci sono per ogni codice.
    printSection("Riepilogo per codice paese")
    df.groupBy("cntry_cd").count().orderBy("cntry_cd").show(MaxRowsToShow, truncate = false)

    // Chiude la SparkSession.
    spark.stop()
  }
}
