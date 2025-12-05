package sparkPratica

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object obj_jsonFlatten {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Flatten JSON Example")
      .master("local[*]")
      .getOrCreate()

    // ===== 🟦 FASE 1: Lettura JSON complesso =====
    println("\n🟦 [FASE 1] Leggo il file JSON complesso:")
    val complexDf = spark.read
      .format("json")
      .option("multiLine", true)
      .load("/home/riccardo/Documenti/repository/spark/spark/random_user.json")

    complexDf.printSchema()

    // ===== 🟩 FASE 2: Appiattimento array 'results' =====
    println("\n🟩 [FASE 2] Appiattisco l'array 'results'...")
    val flatDf = complexDf.withColumn("result", explode(col("results")))

    // ===== 🟨 FASE 3: Selezione colonne annidate =====
    println("\n🟨 [FASE 3] Seleziono alcune colonne annidate come esempio:")
    val selectedDf = flatDf.select(
      col("nationality"),
      col("result.gender"),
      col("result.email"),
      col("result.name.first").alias("first_name"),
      col("result.name.last").alias("last_name"),
      col("result.location.city").alias("city"),
      col("result.location.state").alias("state")
    )

    selectedDf.show(false)

    spark.stop()
  }
}
