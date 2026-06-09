import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions.trim
import org.apache.spark.sql.types._

object ReadingFileWithoutHeader2 {
  def main(args: Array[String]): Unit = {
    // SparkSession configurata con SparkConf: qui non serve SparkContext esplicito.
    val conf = new SparkConf().setAppName("depl").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    // Definizione dello schema manuale:
    // utile quando il file non ha header oppure quando non vuoi affidarti a inferSchema.
    val dml = StructType(Array(
      StructField("state", StringType, true),
      StructField("capital", StringType, true),
      StructField("language", StringType, true),
      StructField("cntry_cd", StringType, true)
    ))

    // Lettura dei file senza header dalla cartella
    val df = spark.read
      .option("header", "false")
      .schema(dml)
      .csv("/home/riccardo/Documenti/repository/spark/spark/country")

    // Mostra fino a 100 righe per verificare che schema e separatore siano corretti.
    df.show(100)

    // 1. Tutto in un unico file CSV in una directory
    // coalesce(1) riduce a una sola partizione, quindi Spark scrive un solo part file.
    // Da usare solo su dataset piccoli, perche' concentra i dati su un solo executor.
    df.coalesce(1)
      .write
      .mode("overwrite")
      .format("csv")
      .save("file:///home/riccardo/Documenti/repository/spark/spark/output_one_dir")


    // 2. Partizionamento per codice paese con pulizia degli spazi
    import org.apache.spark.sql.functions.trim

    // trim rimuove spazi indesiderati: evita directory tipo "cntry_cd= IND".
    val df_clean = df.withColumn("cntry_cd", trim($"cntry_cd"))
    df_clean.coalesce(1)
      .write
      .mode("overwrite")
      .format("csv")
      // partitionBy crea una cartella per ogni valore di cntry_cd.
      .partitionBy("cntry_cd")
      .save("file:///home/riccardo/Documenti/repository/spark/spark/output_by_country")

    // 3. Partizionamento per codice paese e lingua con pulizia degli spazi
    // Partizionare su piu colonne crea una struttura gerarchica: cntry_cd=.../language=...
    val df_clean_lang = df.withColumn("cntry_cd", trim($"cntry_cd")).withColumn("language", trim($"language"))
    df_clean_lang.coalesce(1)
      .write
      .mode("overwrite")
      .format("csv")
      .partitionBy("cntry_cd", "language")
      .save("file:///home/riccardo/Documenti/repository/spark/spark/output_by_country_language")

    spark.stop()
  }
}
