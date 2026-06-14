// per lanciare: sbt "runMain sparkPractise.obj_SchemaRDD"
//
// Scopo dello script
// ------------------
// Questo script dimostra due modi diversi per creare DataFrame con schema:
// 1. usando una case class, dove lo schema deriva dai campi della classe;
// 2. usando Row + StructType, dove lo schema viene definito manualmente.
//
// L'obiettivo e' mostrare come trasformare dati grezzi letti da un file di testo
// in dati strutturati, interrogabili con select, filter e query SQL.
//
// Esempio prima/dopo
// ------------------
// Riga input:
// Andhra Pradesh,Amaravati,Telugu,IND
//
// Dopo split:
// Array("Andhra Pradesh", "Amaravati", "Telugu", "IND")
//
// Dopo case class o Row + StructType:
// state          | capital   | language | country
// Andhra Pradesh | Amaravati | Telugu   | IND
//
// Questo file contiene un esempio di come creare un DataFrame con schema a partire da un file di testo, utilizzando sia una case class che Row + StructType. Assicurati di avere un file di testo chiamato "india.txt" nella directory specificata, o modifica il percorso del file di conseguenza. Il file dovrebbe essere strutturato con righe del tipo: "state,capital,language,country".
// Questo script mostra come dare una struttura ai dati grezzi, permettendo di lavorare con colonne nominate e tipi di dati, invece di trattare i dati come semplici stringhe. Vengono mostrati filtri e query SQL su entrambi i DataFrame creati, e infine i risultati filtrati vengono salvati in un nuovo file di output.

package sparkPractise

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.hadoop.fs.{FileSystem, Path}

case class FileDml(state: String, capital: String, language: String, country: String)

object obj_SchemaRDD {
  private def printSection(title: String): Unit = {
    println()
    println("=" * 90)
    println(title)
    println("=" * 90)
  }

  def main(arg: Array[String]): Unit = {
    printSection("AVVIO - Creazione DataFrame con schema")
    println("Obiettivo: creare DataFrame sia da case class sia da Row + StructType.")
    println("Input: C:\\repository\\spark\\1.input\\India.txt")

    // SparkContext legge i dati come RDD, SparkSession abilita DataFrame e SQL.
    val conf = new SparkConf().setAppName("TestLog").setMaster("local[*]")
    val sc   = new SparkContext(conf)
    sc.setLogLevel("Error")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    printSection("1 - Lettura file di testo come RDD")
    println("SparkContext legge India.txt come RDD di righe.")
    // Percorso di input; aggiorna se il file si trova altrove
    val inputFile = sc.textFile("C:\\repository\\spark\\1.input\\India.txt")
    println(s"Numero righe lette: ${inputFile.count()}")

    // Split per colonna e mapping in case class
    // Se il file ha solo 3 colonne, assegniamo un valore di default a country.
    printSection("2 - Split delle righe in colonne")
    println("Ogni riga viene divisa con split(\",\") e gli spazi vengono rimossi con trim.")
    println("Esempio: Andhra Pradesh,Amaravati,Telugu,IND -> Array(Andhra Pradesh, Amaravati, Telugu, IND)")
    val inputSplit = inputFile.map(line => line.split(",", -1).map(_.trim))

    // La case class produce un DataFrame con schema implicito: colonne = campi della case class.
    printSection("3 - Creazione DataFrame tramite case class")
    println("La case class FileDml assegna nomi e tipi alle colonne in modo automatico.")
    val inputColumns = inputSplit.map { cols =>
      val countryValue = if (cols.length >= 4 && cols(3).nonEmpty) cols(3) else "N/A"
      FileDml(cols(0), cols(1), cols(2), countryValue)
    }
    println("Prime righe convertite in case class:")
    inputColumns.foreach(println)

    // DataFrame con schema (classe case)
    // cache evita di ricalcolare il DataFrame quando viene usato in piu action successive.
    val df = inputColumns.toDF().cache()
    println("Schema DataFrame creato da case class:")
    df.printSchema()

    printSection("4 - Select su DataFrame da case class")
    println("Selezioniamo colonne specifiche: state, capital, language, country.")
    df.select("state", "capital", "language", "country").show(truncate = false)

    printSection("5 - Filtro con DSL su DataFrame da case class")
    println("Filtriamo le righe dove language = English.")
    println("Prima: tutte le lingue. Dopo: rimangono solo le righe con language=English.")
    val englishDf = df.filter($"language" === "English")
    englishDf.show(truncate = false)

    printSection("6 - Query SQL su temp view")
    println("Registriamo il DataFrame come vista temporanea e interroghiamo con SQL.")
    // La temp view consente di interrogare il DataFrame con sintassi SQL.
    df.createOrReplaceTempView("country_table")
    spark.sql("SELECT state, capital, language, country FROM country_table WHERE language = 'Hindi'").show(truncate = false)

    // Salva i risultati nella directory di output configurata per il progetto.
    printSection("7 - Scrittura filtro English in output CSV")
    println("Scriviamo il risultato filtrato in una sola partizione con mode=overwrite.")
    val outputPath = "C:\\repository\\spark\\2.output"
    println(s"Output: $outputPath")
    val fs         = FileSystem.get(sc.hadoopConfiguration)
    val path       = new Path(outputPath)
    if (fs.exists(path)) {
      fs.delete(path, true) // elimina l'output precedente se già esiste
    }
    
    // coalesce(1) produce un solo file dati in output; comodo per esercizi, meno adatto a grandi dataset.
    englishDf.coalesce(1).write.mode("overwrite").csv(outputPath)

    // DataFrame costruito da Row e StructType
    // Questo approccio e' utile quando non vuoi o non puoi definire una case class.
    printSection("8 - Creazione DataFrame tramite Row + StructType")
    println("Costruiamo Row manualmente e definiamo lo schema con StructType.")
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

    println("Schema DataFrame (Row + StructType):")
    structDf.printSchema()
    printSection("9 - Select su DataFrame Row + StructType")
    println("Selezioniamo state e capital.")
    structDf.select("state", "capital").show(truncate = false)

    printSection("10 - Filtro con DSL su DataFrame Row + StructType")
    println("Filtriamo le righe dove language = English.")
    structDf.filter($"language" === "English").show(truncate = false)

    printSection("11 - Query SQL su DataFrame Row + StructType")
    println("Registriamo structDf come vista temporanea e interroghiamo con SQL.")
    structDf.createOrReplaceTempView("struct_country")
    spark.sql("SELECT state, capital, language, country FROM struct_country WHERE language = 'Hindi' LIMIT 2").show(truncate = false)

    printSection("FINE - Job completato")
    println("Sono stati confrontati schema da case class e schema manuale con StructType.")
  }
}
