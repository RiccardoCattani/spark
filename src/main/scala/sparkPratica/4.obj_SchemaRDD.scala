// per lanciare: sbt "runMain sparkPractise.obj_SchemaRDD"
// Questo file contiene un esempio di come creare un DataFrame con schema a partire da un file di testo, utilizzando sia una case class che Row + StructType. Assicurati di avere un file di testo chiamato "india.txt" nella directory specificata, o modifica il percorso del file di conseguenza. Il file dovrebbe essere strutturato con righe del tipo: "state,capital,language,country".

package sparkPractise

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.hadoop.fs.{FileSystem, Path}

case class FileDml(state: String, capital: String, language: String, country: String)

object obj_SchemaRDD {
  def main(arg: Array[String]): Unit = {
    // SparkContext legge i dati come RDD, SparkSession abilita DataFrame e SQL.
    val conf = new SparkConf().setAppName("TestLog").setMaster("local[*]")
    val sc   = new SparkContext(conf)
    sc.setLogLevel("Error")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    // Percorso di input; aggiorna se il file si trova altrove
    val inputFile = sc.textFile("C:\\repository\\spark\\1.input\\India.txt")

    // Split per colonna e mapping in case class
    // Se il file ha solo 3 colonne, assegniamo un valore di default a country.
    val inputSplit = inputFile.map(line => line.split(",", -1).map(_.trim))

    // La case class produce un DataFrame con schema implicito: colonne = campi della case class.
    val inputColumns = inputSplit.map { cols =>
      val countryValue = if (cols.length >= 4 && cols(3).nonEmpty) cols(3) else "N/A"
      FileDml(cols(0), cols(1), cols(2), countryValue)
    }
    inputColumns.foreach(println)

    // DataFrame con schema (classe case)
    // cache evita di ricalcolare il DataFrame quando viene usato in piu action successive.
    val df = inputColumns.toDF().cache()
    df.printSchema()

    println("Selezione colonne (schema DataFrame)")
    df.select("state", "capital", "language", "country").show(truncate = false)

    println("Filtro con DSL (schema DataFrame)")
    val englishDf = df.filter($"language" === "English")
    englishDf.show(truncate = false)

    println("Query SQL su temp view (schema DataFrame)")
    // La temp view consente di interrogare il DataFrame con sintassi SQL.
    df.createOrReplaceTempView("country_table")
    spark.sql("SELECT state, capital, language, country FROM country_table WHERE language = 'Hindi'").show(truncate = false)

    // Salva i risultati in un unico file; aggiorna il path per Windows/Linux
    val outputPath = "C:\\repository\\spark\\2.outputEnglish_1"
    val fs         = FileSystem.get(sc.hadoopConfiguration)
    val path       = new Path(outputPath)
    if (fs.exists(path)) {
      fs.delete(path, true) // elimina l'output precedente se già esiste
    }
    
    // coalesce(1) produce un solo file dati in output; comodo per esercizi, meno adatto a grandi dataset.
    englishDf.coalesce(1).write.mode("overwrite").csv(outputPath)

    // DataFrame costruito da Row e StructType
    // Questo approccio e' utile quando non vuoi o non puoi definire una case class.
    val rowRdd = inputSplit.map { cols =>
      val countryValue = if (cols.length >= 4 && cols(3).nonEmpty) cols(3) else "N/A"
      Row(cols(0), cols(1), cols(2), countryValue)
    }
    // Definisce lo schema del DataFrame usando StructType e StructField, specificando i nomi e i tipi delle colonne.
    val structSchema = StructType(
      List(
        StructField("state", StringType, nullable = true),
        StructField("capital", StringType, nullable = true),
        StructField("language", StringType, nullable = true),
        StructField("country", StringType, nullable = true)
      )
    )

    val structDf = spark.createDataFrame(rowRdd, structSchema).cache()

    println("Schema DataFrame (Row + StructType)")
    structDf.printSchema()
    println("Select su structDf")
    structDf.select("state", "capital").show(truncate = false)

    println("Filtro con DSL su structDf")
    structDf.filter($"language" === "English").show(truncate = false)

    println("Query SQL su temp view (Row DataFrame)")
    structDf.createOrReplaceTempView("struct_country")
    spark.sql("SELECT state, capital, language, country FROM struct_country WHERE language = 'Hindi' LIMIT 2").show(truncate = false)
  }
}
