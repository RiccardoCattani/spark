package sparkPractise

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

case class filedml(state: String, capital: String, language: String, country: String)

object obj_DataFrameFromRDD {
  def main(arg: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Job1").setMaster("local[*]")
    val sc   = new SparkContext(conf)
    sc.setLogLevel("Error")

    // invoke a SparkSession
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    // Percorso di input; aggiorna se il file si trova altrove
    val inputFile  = sc.textFile("file:///C:/data/countries.txt")
    val inputSplit = inputFile.map(x => x.split(","))

    println("=========== schema rdd ============")
    val inputColumns = inputSplit.map(x => filedml(x(0), x(1), x(2), x(3)))

    val dataframe_schema = inputColumns.toDF()
    dataframe_schema.printSchema()
    dataframe_schema.show(false)
    // val sel_col = dataframe_schema.select("", "")

    spark.stop()
    sc.stop()
  }
}
