package sparkPratica

import org.apache.spark.sql.SparkSession

object ReadJsonExample {
  def main(args: Array[String]): Unit = {
    // SparkSession locale per usare la DataFrame API sui file JSON.
    // Spark e' in grado di inferire lo schema, quindi legge campi semplici,
    // struct annidate e array senza definire manualmente uno StructType.
    val spark = SparkSession.builder()
      .appName("Read JSON Example")
      .master("local[*]")
      .getOrCreate()

    // Lettura JSON semplice.
    // Per impostazione predefinita Spark si aspetta JSON Lines: ogni riga deve
    // essere un documento JSON valido e indipendente. Questo formato e' molto
    // usato nei sistemi distribuiti perche' permette di dividere il file in parti.
    println("Leggo il file JSON semplice:")
    val simpleDf = spark.read.format("json").load("/home/riccardo/Documenti/repository/spark/spark/user.json")
    simpleDf.printSchema()
    simpleDf.show(false)

    // Lettura JSON complesso su piu' righe.
    // multiLine=true serve quando il file contiene un singolo documento JSON
    // formattato su piu' righe, per esempio con oggetti annidati e array.
    // Senza questa opzione Spark tenterebbe di interpretare ogni riga separatamente.
    println("Leggo il file JSON complesso:")
    val complexDf = spark.read
      .format("json")
      .option("multiLine", true)
      .load("/home/riccardo/Documenti/repository/spark/spark/random_user.json")

    // printSchema e show(false) sono utili per capire come Spark ha interpretato
    // la struttura: campi struct, array, stringhe, numeri e possibili valori null.
    // false evita il troncamento delle colonne lunghe.
    complexDf.printSchema()
    complexDf.show(false)

    spark.stop()
  }
}
