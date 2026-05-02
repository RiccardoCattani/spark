package sparkPractise

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object adding_removing_updating_Cols {
  def main(arg: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("bank_Trans").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val read_csv_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("C:\\Users\\Riccardo\\Downloads\\2010-12-01.csv")

    read_csv_df.printSchema()
    read_csv_df.persist()
    read_csv_df.show()
  }


}

