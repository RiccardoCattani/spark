package sparkPratica

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object adding_removing_updating_Cols {
  def main(arg: Array[String]): Unit = {
    // Configurazione per eseguire Spark in locale usando tutti i core disponibili.
    val conf = new SparkConf().setAppName("bank_Trans").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")

    // SparkSession e' l'entry point per lavorare con DataFrame e funzioni SQL.
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    // Legge il CSV delle transazioni bancarie:
    // - header=true usa la prima riga come nome colonne
    // - inferSchema=true prova a riconoscere automaticamente tipi numerici/date/stringhe
    val read_csv_df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("C:\\Users\\Riccardo\\Downloads\\2010-12-01.csv")

    // printSchema mostra i tipi dedotti; utile per capire se inferSchema ha letto correttamente il file.
    read_csv_df.printSchema()

    // persist mantiene il DataFrame in cache dopo la prima action, utile se lo riusi piu volte.
    read_csv_df.persist()

    // show esegue una action e stampa le prime righe sul driver.
    read_csv_df.show()
  }


}

