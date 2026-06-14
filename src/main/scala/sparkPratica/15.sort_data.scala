package sparkPratica

// sbt "runMain sparkPratica.obj_sort_data"
//
// Scopo dello script
// ------------------
// Questo script mostra come ordinare un DataFrame Spark.
//
// Il video di riferimento mostra un esempio chiamato sortData.scala sul dataset
// bank_transactions.csv. La parte centrale e':
//
// read_csv_df.orderBy(col("CustGender").desc, col("CustLocation").asc)
//
// e poi una variante:
//
// read_csv_df.orderBy(col("CustLocation").asc_nulls_first)
//
// In questo script riprendiamo lo stesso concetto e lo rendiamo piu didattico:
// - lettura del CSV bancario;
// - selezione di poche colonne utili per vedere meglio l'ordinamento;
// - sort/orderBy crescente;
// - sort/orderBy decrescente;
// - ordinamento su piu colonne;
// - gestione dei valori null con asc_nulls_first e asc_nulls_last.
//
// Differenza tra sort e orderBy
// -----------------------------
// Nei DataFrame Spark, sort(...) e orderBy(...) sono equivalenti per l'uso
// comune. Entrambi restituiscono un nuovo DataFrame ordinato.
//
// Esempio:
//
// df.sort(col("CustLocation").asc)
// df.orderBy(col("CustLocation").asc)
//
// producono lo stesso tipo di risultato.

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object obj_sort_data {
  private val MaxRowsToShow = 40
  private val BankTransactionsPath = "C:\\repository\\spark\\1.input\\bank_transactions.csv"

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

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("sort-data")
      .setMaster("local[*]")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    printSection("AVVIO - Ordinamento DataFrame")
    println("Leggo bank_transactions.csv e mostro diversi modi per ordinare i dati.")

    // Lettura del CSV originale.
    //
    // Prima, file fisico:
    //
    // TransactionID,CustomerID,CustomerDOB,CustGender,CustLocation,CustAccountBalance,TransactionDate,TransactionTime,TransactionAmount (INR)
    // T1,C5841053,10/1/94,F,JAMSHEDPUR,17819.05,2/8/16,143207,25
    //
    // Dopo read.csv:
    //
    // TransactionID | CustomerID | CustGender | CustLocation | CustAccountBalance | TransactionDate | TransactionAmount (INR)
    // T1            | C5841053   | F          | JAMSHEDPUR   | 17819.05           | 2/8/16          | 25.0
    val read_csv_df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(BankTransactionsPath)
      .persist()

    showDataFrameDetails("1 - CSV originale limitato alle prime righe", read_csv_df.limit(10))

    // Selezioniamo solo alcune colonne per rendere piu leggibile l'output.
    //
    // Prima:
    //
    // TransactionID | CustomerID | CustomerDOB | CustGender | CustLocation | CustAccountBalance | TransactionDate | TransactionTime | TransactionAmount (INR)
    // T1            | C5841053   | 10/1/94     | F          | JAMSHEDPUR   | 17819.05           | 2/8/16          | 143207          | 25.0
    //
    // Dopo:
    //
    // TransactionID | CustomerID | CustGender | CustLocation | CustAccountBalance | TransactionDate | Transaction_Amount
    // T1            | C5841053   | F          | JAMSHEDPUR   | 17819.05           | 2/8/16          | 25.0
    val selected_df = read_csv_df
      .select(
        col("TransactionID"),
        col("CustomerID"),
        col("CustGender"),
        col("CustLocation"),
        col("CustAccountBalance"),
        col("TransactionDate"),
        col("TransactionAmount (INR)").alias("Transaction_Amount")
      )
      .limit(1000)
      .persist()

    showDataFrameDetails("2 - DataFrame selezionato per esempi di sort", selected_df)

    // Ordinamento crescente con orderBy.
    //
    // orderBy(col("CustLocation").asc) ordina alfabeticamente la localita'.
    //
    // Prima:
    //
    // TransactionID | CustLocation
    // T1            | JAMSHEDPUR
    // T3            | MUMBAI
    // T13           | AHMEDABAD
    //
    // Dopo:
    //
    // TransactionID | CustLocation
    // T13           | AHMEDABAD
    // T1            | JAMSHEDPUR
    // T3            | MUMBAI
    val location_asc_df = selected_df
      .orderBy(col("CustLocation").asc)

    showDataFrameDetails("3 - orderBy CustLocation crescente", location_asc_df)

    // Ordinamento decrescente con orderBy.
    //
    // desc inverte l'ordinamento: Z prima di A.
    //
    // Prima:
    //
    // AHMEDABAD, JAMSHEDPUR, MUMBAI
    //
    // Dopo:
    //
    // MUMBAI, JAMSHEDPUR, AHMEDABAD
    val location_desc_df = selected_df
      .orderBy(col("CustLocation").desc)

    showDataFrameDetails("4 - orderBy CustLocation decrescente", location_desc_df)

    // Ordinamento su piu colonne, come nel video.
    //
    // orderBy(col("CustGender").desc, col("CustLocation").asc)
    //
    // Spark ordina prima per CustGender in modo decrescente.
    // Quindi normalmente mette M prima di F.
    // A parita' di CustGender, ordina CustLocation in modo crescente.
    //
    // Prima:
    //
    // TransactionID | CustGender | CustLocation
    // T1            | F          | JAMSHEDPUR
    // T2            | M          | JHAJJAR
    // T8            | M          | MUMBAI
    // T13           | M          | AHMEDABAD
    //
    // Dopo:
    //
    // TransactionID | CustGender | CustLocation
    // T13           | M          | AHMEDABAD
    // T2            | M          | JHAJJAR
    // T8            | M          | MUMBAI
    // T1            | F          | JAMSHEDPUR
    val gender_desc_location_asc_df = selected_df
      .orderBy(col("CustGender").desc, col("CustLocation").asc)

    showDataFrameDetails("5 - orderBy CustGender desc e CustLocation asc", gender_desc_location_asc_df)

    // Variante vista nel video: due colonne entrambe decrescenti.
    //
    // orderBy(col("CustGender").desc, col("CustLocation").desc)
    //
    // Prima ordina per genere decrescente, poi per localita' decrescente.
    //
    // Esempio:
    //
    // M | MUMBAI
    // M | JHAJJAR
    // M | AHMEDABAD
    // F | JAMSHEDPUR
    val gender_desc_location_desc_df = selected_df
      .orderBy(col("CustGender").desc, col("CustLocation").desc)

    showDataFrameDetails("6 - orderBy CustGender desc e CustLocation desc", gender_desc_location_desc_df)

    // sort e orderBy sono equivalenti nell'uso comune.
    //
    // Qui ordiniamo per importo crescente usando sort.
    //
    // Prima:
    //
    // TransactionID | Transaction_Amount
    // T1            | 25.0
    // T2            | 27999.0
    // T3            | 459.0
    //
    // Dopo:
    //
    // TransactionID | Transaction_Amount
    // T1            | 25.0
    // T3            | 459.0
    // T2            | 27999.0
    val amount_asc_df = selected_df
      .sort(col("Transaction_Amount").asc)

    showDataFrameDetails("7 - sort Transaction_Amount crescente", amount_asc_df)

    // Ordinamento per importo decrescente.
    //
    // Utile quando vogliamo vedere prima le transazioni piu grandi.
    //
    // Prima:
    //
    // 25.0, 27999.0, 459.0
    //
    // Dopo:
    //
    // 27999.0, 459.0, 25.0
    val amount_desc_df = selected_df
      .sort(col("Transaction_Amount").desc)

    showDataFrameDetails("8 - sort Transaction_Amount decrescente", amount_desc_df)

    // Gestione dei null nell'ordinamento.
    //
    // Nel dataset alcune righe hanno CustLocation null.
    // asc_nulls_first mette i null all'inizio.
    //
    // Prima:
    //
    // CustLocation
    // JAMSHEDPUR
    // null
    // AHMEDABAD
    //
    // Dopo asc_nulls_first:
    //
    // CustLocation
    // null
    // AHMEDABAD
    // JAMSHEDPUR
    val location_nulls_first_df = selected_df
      .orderBy(col("CustLocation").asc_nulls_first)

    showDataFrameDetails("9 - orderBy CustLocation asc_nulls_first", location_nulls_first_df)

    // asc_nulls_last mette invece i null alla fine.
    //
    // Prima:
    //
    // CustLocation
    // null
    // AHMEDABAD
    // JAMSHEDPUR
    //
    // Dopo asc_nulls_last:
    //
    // CustLocation
    // AHMEDABAD
    // JAMSHEDPUR
    // null
    val location_nulls_last_df = selected_df
      .orderBy(col("CustLocation").asc_nulls_last)

    showDataFrameDetails("10 - orderBy CustLocation asc_nulls_last", location_nulls_last_df)

    printSection("FINE - Job completato")
    println("Lo script ha mostrato orderBy, sort, asc, desc, ordinamento su piu colonne e gestione dei null.")

    spark.stop()
  }
}
