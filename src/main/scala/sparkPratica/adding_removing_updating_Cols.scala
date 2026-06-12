// Scopo dello script
// ------------------
// Questo script dimostra alcune operazioni di analisi preliminare su un DataFrame
// Spark letto da CSV. L'obiettivo non e' trasformare i dati, ma controllarne la
// struttura e la qualita'.
//
// In particolare lo script:
// - legge un CSV di transazioni bancarie con header e inferenza dello schema;
// - mostra colonne, schema, numero righe e dati di esempio;
// - calcola statistiche descrittive con describe();
// - conta, per ogni colonna, quanti valori sono null o vuoti.
//
// Serve quindi come esempio di data profiling iniziale prima di applicare
// trasformazioni o pulizia piu' avanzata.
//
package sparkPratica

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Questo script legge un CSV di transazioni bancarie e mostra operazioni utili
// per ispezionare un DataFrame: schema, dati, statistiche descrittive e conteggio
// dei valori null o vuoti per ogni colonna.
object adding_removing_updating_Cols {
  private val MaxRowsToShow = 100

  private def printSection(title: String): Unit = {
    println()
    println("=" * 90)
    println(title)
    println("=" * 90)
  }

  private def showDataFrameDetails(title: String, df: DataFrame): Unit = {
    printSection(title)
    val totalRows = df.count()
    val rowsToShow = math.min(totalRows, MaxRowsToShow).toInt
    println(s"Numero colonne: ${df.columns.length}")
    println(s"Colonne: ${df.columns.mkString(", ")}")
    println(s"Numero righe: $totalRows")
    println("Schema:")
    df.printSchema()
    println(s"Dati mostrati: $rowsToShow righe su $totalRows")
    df.show(rowsToShow, truncate = false)
  }

  def main(arg: Array[String]): Unit = {
    // Configura Spark in locale e crea lo SparkContext.
    val conf = new SparkConf().setAppName("bank_Trans").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")

    // Crea la SparkSession per leggere il CSV come DataFrame.
    val spark = SparkSession.builder().getOrCreate()

    // Legge il CSV con header e inferSchema.
    // inferSchema prova a riconoscere automaticamente i tipi delle colonne.
    val read_csv_df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("C:\\Users\\Riccardo\\Downloads\\2010-12-01.csv")
      .persist()

    showDataFrameDetails("CSV transazioni bancarie letto con inferSchema", read_csv_df)

    // describe calcola statistiche base come count, mean, stddev, min e max.
    printSection("Statistiche descrittive colonne numeriche/stringa")
    read_csv_df.describe().show(MaxRowsToShow, truncate = false)

    // Per ogni colonna conta i valori null o stringhe vuote.
    printSection("Conteggio valori null per colonna")
    val nullCounts = read_csv_df.columns.map { columnName =>
      sum(when(col(columnName).isNull || trim(col(columnName).cast("string")) === "", 1).otherwise(0)).alias(columnName)
    }
    read_csv_df.select(nullCounts: _*).show(truncate = false)

    // Chiude SparkSession e SparkContext.
    spark.stop()
    sc.stop()
  }
}
