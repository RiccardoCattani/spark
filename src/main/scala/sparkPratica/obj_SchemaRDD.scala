package sparkPractise

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.hadoop.fs.{FileSystem, Path}

object obj_SchemaRDD {
  def main(arg: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestLog").setMaster("local[*]")
    val sc   = new SparkContext(conf)
    sc.setLogLevel("Error")

    // Percorso di input; aggiorna se il file si trova altrove
    val inputFile = sc.textFile("/home/riccardo/Documenti/spark/countries.txt")

    // Split per colonna e mapping in Row
    val inputSplit   = inputFile.map(x => x.split(","))
    val inputColumns = inputSplit.map(x => Row(x(0), x(1), x(2), x(3)))
    inputColumns.foreach(println)

    // Filtra le righe dove la lingua contiene "English"
    val filData = inputColumns.filter(x => x(2).toString.contains("English"))
    filData.foreach(println)

    // Salva i risultati in un unico file; aggiorna il path per Windows/Linux
    val outputPath = "/home/riccardo/Documenti/spark/output/English_1"
    val fs         = FileSystem.get(sc.hadoopConfiguration)
    val path       = new Path(outputPath)
    if (fs.exists(path)) {
      fs.delete(path, true) // elimina l'output precedente se già esiste
    }
    filData.coalesce(1).saveAsTextFile(outputPath)
  }
}
