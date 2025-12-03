package sparkPractise

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object obj_WordCount {
  def main(arg: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestLog").setMaster("local[*]")
    val sc   = new SparkContext(conf)
    sc.setLogLevel("Error")

    val inputRDD = sc.textFile("file:///C:/data/words.txt")
    val word_count = inputRDD
      .flatMap(s => s.split(" "))
      .map(word => (word, 1))
      .reduceByKey((x, y) => (x + y))

    word_count.collect().foreach(println)
  }
}
