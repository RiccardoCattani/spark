import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.types._

object ReadingFileWithoutHeader {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("depl").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    // Definizione dello schema manuale
    val dml = StructType(Array(
      StructField("state", StringType, true),
      StructField("capital", StringType, true),
      StructField("language", StringType, true),
      StructField("cntry_cd", StringType, true)
    ))

    // Lettura del file senza header
    val df = spark.read
      .option("header", "false")
      .schema(dml)
      .csv("/home/riccardo/Documenti/repository/spark/sparkfile.csv")

    df.show()
    spark.stop()
  }
}