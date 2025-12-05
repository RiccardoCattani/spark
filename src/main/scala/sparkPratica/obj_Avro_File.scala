package sparkPratica

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object obj_Avro_File {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("job1").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")

    val spark = SparkSession.builder().getOrCreate()

    val read_df = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", "|")
      .load("file:///C:/data/India_pipe.txt")

    // Puoi aggiungere qui altre operazioni sul DataFrame
    read_df.show(5)
  }
}
