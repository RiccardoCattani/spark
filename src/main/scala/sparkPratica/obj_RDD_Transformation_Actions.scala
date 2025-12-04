
package sparkPractise
object obj_RDD_Transformation_Actions
import org.apache.spark.{SparkConf, SparkContext}

object obj_logs {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Logs Analysis").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")

    val inputRDD = sc.textFile("C:/SparkScala/SparkScalaPractise/src/main/scala/sparkPractise/logs/logs.txt")
    // ...qui puoi aggiungere altre operazioni su inputRDD...
  }
}