package sparkPratica

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object obj_jsonFlatten {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Flatten JSON Example")
      .master("local[*]")
      .getOrCreate()

    // FASE 1: lettura del JSON complesso.
    // Il file contiene campi annidati, quindi prima conviene osservare lo schema
    // generato da Spark. In questo modo si capisce quali campi sono array,
    // quali sono struct e quali sono valori semplici.
    println("\n[FASE 1] Leggo il file JSON complesso:")
    val complexDf = spark.read
      .format("json")
      .option("multiLine", true)
      .load("/home/riccardo/Documenti/repository/spark/spark/random_user.json")

    complexDf.printSchema()

    // FASE 2: appiattimento dell'array results.
    // explode crea una nuova riga per ogni elemento dell'array. Se results contiene
    // 10 elementi, una riga del DataFrame originale diventa 10 righe nel DataFrame
    // risultante. Ogni elemento esploso viene salvato nella colonna "result".
    println("\n[FASE 2] Appiattisco l'array 'results'...")
    val flatDf = complexDf.withColumn("result", explode(col("results")))

    // FASE 3: selezione di campi annidati.
    // La dot notation permette di navigare dentro struct annidate:
    // result.user.name.first significa: colonna result -> campo user -> campo name -> campo first.
    // alias rinomina le colonne finali per ottenere un DataFrame piu' leggibile.
    println("\n[FASE 3] Seleziono alcune colonne annidate come esempio:")
    val selectedDf = flatDf.select(
      col("nationality"),
      col("result.user.gender"),
      col("result.user.email"),
      col("result.user.name.first").alias("first_name"),
      col("result.user.name.last").alias("last_name"),
      col("result.user.location.city").alias("city"),
      col("result.user.location.state").alias("state")
    )

    // truncate=false stampa il contenuto completo delle colonne selezionate.
    // E' utile quando email, indirizzi o campi JSON lunghi verrebbero tagliati.
    selectedDf.show(false)

    spark.stop()
  }
}
