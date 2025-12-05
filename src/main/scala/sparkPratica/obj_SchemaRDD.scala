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
    val conf = new SparkConf().setAppName("TestLog").setMaster("local[*]")
    val sc   = new SparkContext(conf)
    sc.setLogLevel("Error")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    // Percorso di input; aggiorna se il file si trova altrove
    val inputFile = sc.textFile("/home/riccardo/Documenti/spark/countries.txt")

    // Split per colonna e mapping in case class
    val inputSplit   = inputFile.map(x => x.split(","))
    val inputColumns = inputSplit.map(x => FileDml(x(0), x(1), x(2), x(3)))
    inputColumns.foreach(println)

    // DataFrame con schema (classe case)
    val df = inputColumns.toDF().cache()
    df.printSchema()

    println("Selezione colonne (schema DataFrame)")
    df.select("state", "capital", "language", "country").show(truncate = false)

    println("Filtro con DSL (schema DataFrame)")
    val englishDf = df.filter($"language" === "English")
    englishDf.show(truncate = false)

    println("Query SQL su temp view (schema DataFrame)")
    df.createOrReplaceTempView("country_table")
    spark.sql("SELECT state, capital, language, country FROM country_table WHERE language = 'Hindi'").show(truncate = false)

    // Salva i risultati in un unico file; aggiorna il path per Windows/Linux
    val outputPath = "/home/riccardo/Documenti/spark/output/English_1"
    val fs         = FileSystem.get(sc.hadoopConfiguration)
    val path       = new Path(outputPath)
    if (fs.exists(path)) {
      fs.delete(path, true) // elimina l'output precedente se già esiste
    }
    englishDf.coalesce(1).write.mode("overwrite").csv(outputPath)

    // DataFrame costruito da Row e StructType
    val rowRdd = inputSplit.map(x => Row(x(0), x(1), x(2), x(3)))
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
