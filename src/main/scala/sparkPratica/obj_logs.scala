package sparkPractise
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object obj_Logs {

    def main(arg:Array[String]):Unit=
    {
        val conf=new SparkConf().setAppName("TestLog").setMaster("local[*]")
        val sc=new SparkContext(conf)
        sc.setLogLevel("Error")

        val inputRDD=sc.textFile("file:///C:/data/test_log.txt")
        inputRDD.foreach(println)

    }
}