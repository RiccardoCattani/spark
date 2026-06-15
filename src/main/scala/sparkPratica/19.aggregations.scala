package sparkPratica

// sbt "runMain sparkPratica.obj_aggregations"
//
// Scopo dello script
// ------------------
// Questo script mostra come fare aggregazioni su un DataFrame Spark.
//
// Il video di riferimento usa il file bank_transactions.csv e mostra:
//
// - select di alcune colonne utili;
// - groupBy("CustLocation");
// - agg con sum;
// - agg con sum e count;
// - agg con sum, count, max, min e avg.
//
// In questo script riprendiamo quei passaggi e aggiungiamo commenti prima/dopo
// per rendere chiaro cosa succede quando Spark raggruppa le righe.

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object obj_aggregations {
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
      .setAppName("aggregations")
      .setMaster("local[*]")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    printSection("AVVIO - Aggregations")
    println("Leggo bank_transactions.csv e mostro groupBy, agg, sum, count, max, min e avg.")

    // Lettura del CSV originale.
    //
    // Prima, file fisico:
    //
    // TransactionID,CustomerID,CustomerDOB,CustGender,CustLocation,CustAccountBalance,TransactionDate,TransactionTime,TransactionAmount (INR)
    // T1,C5841053,10/1/94,F,JAMSHEDPUR,17819.05,2/8/16,143207,25
    //
    // Dopo read.csv:
    //
    // TransactionID | CustomerID | CustLocation | CustAccountBalance | TransactionAmount (INR)
    // T1            | C5841053   | JAMSHEDPUR   | 17819.05           | 25.0
    val read_csv_df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(BankTransactionsPath)
      .persist()

    showDataFrameDetails("1 - CSV originale limitato alle prime righe", read_csv_df.limit(10))

    // Selezione delle colonne usate nel video.
    //
    // Prima:
    //
    // TransactionID | CustomerID | CustomerDOB | CustGender | CustLocation | CustAccountBalance | TransactionDate | TransactionTime | TransactionAmount (INR)
    //
    // Dopo:
    //
    // CustLocation | CustAccountBalance | TransactionID
    //
    // Queste colonne bastano per rispondere a domande come:
    // - qual e' il saldo totale per localita'?
    // - quante transazioni ci sono per localita'?
    // - qual e' il saldo massimo, minimo e medio per localita'?
    val df = read_csv_df
      .select(
        col("CustLocation"),
        col("CustAccountBalance"),
        col("TransactionID")
      )
      .persist()

    showDataFrameDetails("2 - Colonne selezionate per le aggregazioni", df.limit(1000))

    // groupBy senza aggregazione finale non produce ancora una tabella visibile.
    //
    // groupBy("CustLocation") prepara i gruppi, ma Spark ha bisogno di sapere
    // quale calcolo fare su ogni gruppo: sum, count, max, min, avg, ecc.
    //
    // Esempio logico:
    //
    // CustLocation | CustAccountBalance
    // MUMBAI       | 17874.44
    // MUMBAI       | 866503.21
    // PUNE         | 10100.84
    //
    // Dopo groupBy("CustLocation"), Spark crea gruppi concettuali:
    //
    // MUMBAI -> 17874.44, 866503.21, ...
    // PUNE   -> 10100.84, ...
    //
    // Poi con agg scegliamo cosa calcolare dentro ogni gruppo.
    printSection("3 - groupBy prepara i gruppi")
    println("df.groupBy(\"CustLocation\") raggruppa le righe per localita', poi agg calcola valori per ogni gruppo.")

    // sum.
    //
    // sum("CustAccountBalance") somma i saldi dei clienti dentro ogni localita'.
    //
    // Prima:
    //
    // CustLocation | CustAccountBalance
    // MUMBAI       | 17874.44
    // MUMBAI       | 866503.21
    //
    // Dopo:
    //
    // CustLocation | Total_Balance
    // MUMBAI       | somma dei saldi delle righe MUMBAI
    val total_balance_df = df
      .groupBy("CustLocation")
      .agg(
        sum(col("CustAccountBalance")).as("Total_Balance")
      )
      .orderBy(col("CustLocation").asc_nulls_last)

    showDataFrameDetails("4 - groupBy + sum: saldo totale per localita'", total_balance_df)

    // count.
    //
    // count("TransactionID") conta quante transazioni hanno un TransactionID
    // non null dentro ogni localita'.
    //
    // Insieme a sum otteniamo:
    //
    // CustLocation | Total_Balance | Total_Transaction
    // MUMBAI       | ...           | numero transazioni MUMBAI
    val total_and_count_df = df
      .groupBy("CustLocation")
      .agg(
        sum(col("CustAccountBalance")).as("Total_Balance"),
        count(col("TransactionID")).as("Total_Transaction")
      )
      .orderBy(col("Total_Transaction").desc, col("CustLocation").asc_nulls_last)

    showDataFrameDetails("5 - groupBy + sum + count", total_and_count_df)

    // max e min.
    //
    // max("CustAccountBalance") prende il saldo piu alto nel gruppo.
    // min("CustAccountBalance") prende il saldo piu basso nel gruppo.
    //
    // Esempio:
    //
    // CustLocation | CustAccountBalance
    // MUMBAI       | 17874.44
    // MUMBAI       | 866503.21
    // MUMBAI       | 973.46
    //
    // Dopo:
    //
    // Max_Balance = 866503.21
    // Min_Balance = 973.46
    val max_min_df = df
      .groupBy("CustLocation")
      .agg(
        max(col("CustAccountBalance")).as("Max_Balance"),
        min(col("CustAccountBalance")).as("Min_Balance")
      )
      .orderBy(col("CustLocation").asc_nulls_last)

    showDataFrameDetails("6 - groupBy + max + min", max_min_df)

    // avg.
    //
    // avg("CustAccountBalance") calcola la media dei saldi dentro ogni gruppo.
    //
    // Nel video la catena finale diventa una agg con piu funzioni insieme:
    // - sum;
    // - count;
    // - max;
    // - min;
    // - avg.
    //
    // Questa e' la forma piu comoda quando vogliamo un riepilogo completo.
    val all_aggregations_df = df
      .groupBy("CustLocation")
      .agg(
        sum(col("CustAccountBalance")).as("Total_Balance"),
        count(col("TransactionID")).as("Total_Transaction"),
        max(col("CustAccountBalance")).as("Max_Balance"),
        min(col("CustAccountBalance")).as("Min_Balance"),
        avg(col("CustAccountBalance")).as("Avg_Balance")
      )
      .orderBy(col("Total_Transaction").desc, col("CustLocation").asc_nulls_last)

    showDataFrameDetails("7 - groupBy + sum + count + max + min + avg", all_aggregations_df)

    // Aggregazione per piu colonne.
    //
    // groupBy puo' ricevere anche piu colonne.
    // Qui raggruppiamo per localita' e contiamo quante transazioni ci sono in
    // ogni localita'. Questo esempio e' uguale al precedente perche' usiamo una
    // sola colonna, ma la sintassi si estende cosi:
    //
    // df.groupBy("CustLocation", "CustGender").agg(...)
    //
    // Nel dataset selezionato non abbiamo CustGender, quindi sotto mostriamo
    // un riepilogo leggermente piu completo partendo dal CSV originale.
    val location_gender_df = read_csv_df
      .select(
        col("CustLocation"),
        col("CustGender"),
        col("CustAccountBalance"),
        col("TransactionID")
      )
      .groupBy("CustLocation", "CustGender")
      .agg(
        count(col("TransactionID")).as("Total_Transaction"),
        sum(col("CustAccountBalance")).as("Total_Balance"),
        avg(col("CustAccountBalance")).as("Avg_Balance")
      )
      .orderBy(col("Total_Transaction").desc, col("CustLocation").asc_nulls_last, col("CustGender").asc_nulls_last)

    showDataFrameDetails("8 - groupBy su piu colonne: CustLocation e CustGender", location_gender_df)

    printSection("FINE - Job completato")
    println("Lo script ha mostrato select, groupBy, agg, sum, count, max, min, avg e groupBy su piu colonne.")

    spark.stop()
  }
}
