import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object obj_SeamlessDataframe {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Job1")
      .setMaster("local[*]")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // Lettura file CSV train.csv
    val read_csv_df = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load("file:///C:/data/train.csv")

    read_csv_df.printSchema()

    println("****************csv data-train(employee)****************")

    read_csv_df.show(5)


    // Lettura file delimitato da pipe |
    val read_pipe_df = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", "|")
      .load("file:///C:/data/india_pipe.txt")

    read_pipe_df.printSchema()

    println("****************pipe delimited data-india****************")

    read_pipe_df.show(5)


    // Scrittura del DataFrame pipe in CSV con altro delimitatore
    println("****************write pipe data to another delimiter****************")

    read_pipe_df.write
      .format("csv")
      .option("header", "true")
      .option("delimiter", "~")
      .mode("overwrite")
      .save("file:///C:/data/output/file_india_csv")


    // Scrittura in formato ORC
    read_pipe_df.write
      .format("orc")
      .mode("overwrite")
      .save("file:///C:/data/output/file_india_orc")


    // Scrittura in formato Parquet
    read_pipe_df.write
      .format("parquet")
      .mode("overwrite")
      .save("file:///C:/data/output/file_india_parquet")


    // Scrittura in formato JSON
    read_pipe_df.write
      .format("json")
      .mode("overwrite")
      .save("file:///C:/data/output/file_india_json")


    // Lettura da ORC
    println("****************read from ORC****************")

    val orc_df = spark.read
      .format("orc")
      .load("file:///C:/data/output/file_india_orc")

    orc_df.show(5)


    // Lettura da Parquet
    println("****************read from Parquet****************")

    val parquet_df = spark.read
      .format("parquet")
      .load("file:///C:/data/output/file_india_parquet")

    parquet_df.show(5)


    // Lettura da JSON
    println("****************read from JSON****************")

    val json_df = spark.read
      .format("json")
      .load("file:///C:/data/output/file_india_json")

    json_df.show(5)


    spark.stop()
  }
}