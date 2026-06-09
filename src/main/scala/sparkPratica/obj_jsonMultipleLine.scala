package sparkPratica

import org.apache.spark.sql.SparkSession

object ReadJsonExample {
  def main(args: Array[String]): Unit = {
    // SparkSession locale per usare la DataFrame API sui file JSON.
    val spark = SparkSession.builder()
      .appName("Read JSON Example")
      .master("local[*]")
      .getOrCreate()

    // Lettura JSON semplice
    println("Leggo il file JSON semplice:")

    // Senza multiLine Spark si aspetta normalmente un record JSON per riga.
    val simpleDf = spark.read.format("json").load("/home/riccardo/Documenti/repository/spark/spark/user.json")
    simpleDf.printSchema()
    simpleDf.show(false)

    // Lettura JSON complesso
    println("Leggo il file JSON complesso:")
    val complexDf = spark.read
      .format("json")
      // multiLine=true permette di leggere un singolo documento JSON distribuito su piu righe.
      .option("multiLine", true)
      .load("/home/riccardo/Documenti/repository/spark/spark/random_user.json")

    // printSchema e show aiutano a capire quali campi sono struct, array o valori semplici.
    complexDf.printSchema()
    complexDf.show(false)

    spark.stop()
  }
}
