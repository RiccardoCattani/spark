## Esempio Schema RDD in Spark (come da screenshot)

```scala
package sparkPractise

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

case class FileDml(
  state: String,
  capital: String,
  language: String,
  country: String
)

object obj_SchemaRDD {
  def main(arg: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestLog").setMaster("local[*]")
    val sc   = new SparkContext(conf)
    sc.setLogLevel("Error")

    // Aggiorna il path in base al tuo sistema operativo
    val inputFile = sc.textFile("file:///C:/data/countries.txt")
    inputFile.foreach(println)
  }
}
```

Note veloci:
- `case class FileDml` modella le colonne del file (stato, capitale, lingua, paese).
- `SparkConf` e `SparkContext` inizializzano l'applicazione Spark locale.
- `textFile` carica il file di input; cambia il percorso se lavori in Linux o con un'altra cartella.
