package sparkPratica

// sbt "runMain sparkPratica.obj_window_functions"
//
// Scopo dello script
// ------------------
// Questo script mostra come usare le Window Functions nei DataFrame Spark.
//
// Il video di riferimento usa il file bank_transactions.csv e mostra:
//
// - Window.partitionBy("CustomerID").orderBy("CustAccountBalance")
// - row_number()
// - rank()
// - dense_rank()
// - percent_rank()
// - lag()
// - lead()
//
// Le Window Functions permettono di fare calcoli "dentro un gruppo" senza
// ridurre il numero di righe come succede con groupBy + agg.
//
// Differenza importante:
//
// groupBy("CustomerID").agg(...)
//   raggruppa e produce una riga per ogni CustomerID.
//
// Window.partitionBy("CustomerID")
//   mantiene tutte le righe originali, ma calcola valori guardando le altre
//   righe dello stesso CustomerID.

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object obj_window_functions {
  private val MaxRowsToShow = 50
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
      .setAppName("window-functions")
      .setMaster("local[*]")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    printSection("AVVIO - Window Functions")
    println("Leggo bank_transactions.csv e mostro row_number, rank, dense_rank, percent_rank, lag e lead.")

    // Lettura del CSV originale.
    //
    // Prima, file fisico:
    //
    // TransactionID,CustomerID,CustomerDOB,CustGender,CustLocation,CustAccountBalance,TransactionDate,TransactionTime,TransactionAmount (INR)
    // T1,C5841053,10/1/94,F,JAMSHEDPUR,17819.05,2/8/16,143207,25
    //
    // Dopo read.csv:
    //
    // TransactionID | CustomerID | CustLocation | CustAccountBalance | TransactionDate | TransactionAmount (INR)
    // T1            | C5841053   | JAMSHEDPUR   | 17819.05           | 2/8/16          | 25.0
    val read_csv_df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(BankTransactionsPath)
      .persist()

    showDataFrameDetails("1 - CSV originale limitato alle prime righe", read_csv_df.limit(10))

    // Selezioniamo le colonne usate negli esempi del video.
    //
    // Castiamo CustAccountBalance a double per essere sicuri che l'ordinamento
    // sia numerico e non alfabetico.
    val df = read_csv_df
      .select(
        col("TransactionID"),
        col("CustomerID"),
        col("CustomerDOB"),
        col("CustGender"),
        col("CustLocation"),
        col("CustAccountBalance").cast("double").alias("CustAccountBalance"),
        col("TransactionDate"),
        col("TransactionTime"),
        col("TransactionAmount (INR)").alias("Transaction_Amount")
      )
      .na.drop(Seq("CustomerID", "CustAccountBalance"))
      .persist()

    showDataFrameDetails("2 - Colonne selezionate per le Window Functions", df.limit(1000))

    // Window specification.
    //
    // Questa riga definisce "come" Spark deve guardare le righe:
    //
    // partitionBy("CustomerID")
    //   crea un gruppo separato per ogni cliente.
    //
    // orderBy("CustAccountBalance")
    //   dentro ogni cliente, ordina le righe per saldo crescente.
    //
    // Esempio logico:
    //
    // CustomerID | CustAccountBalance
    // C1119249  | 6520.75
    // C1119249  | 16406.51
    // C1119249  | 24911.32
    //
    // La finestra del cliente C1119249 contiene solo quelle righe, ordinate per
    // CustAccountBalance.
    val windowSpec = Window
      .partitionBy("CustomerID")
      .orderBy(col("CustAccountBalance").asc)

    printSection("3 - Window specification")
    println("Window.partitionBy(\"CustomerID\").orderBy(\"CustAccountBalance\") crea una finestra per cliente ordinata per saldo.")

    // row_number.
    //
    // row_number() assegna un numero progressivo a ogni riga dentro la finestra.
    //
    // Se un cliente ha 3 righe ordinate per saldo, otteniamo:
    //
    // CustomerID | CustAccountBalance | row_num
    // C1119249  | 6520.75            | 1
    // C1119249  | 16406.51           | 2
    // C1119249  | 24911.32           | 3
    //
    // row_number non lascia buchi: 1, 2, 3, 4...
    val row_number_df = df
      .select(col("CustomerID"), col("CustAccountBalance"))
      .withColumn("row_num", row_number().over(windowSpec))

    showDataFrameDetails("4 - row_number per CustomerID", row_number_df.orderBy("CustomerID", "row_num").limit(100))

    // rank.
    //
    // rank() assegna una posizione dentro la finestra.
    // Se ci sono valori uguali, assegna lo stesso rank e lascia un buco.
    //
    // Esempio:
    //
    // saldo | rank
    // 100   | 1
    // 100   | 1
    // 200   | 3
    //
    // Il valore 200 prende rank 3 perche' due righe sono gia' al primo posto.
    val rank_df = df
      .select(col("CustomerID"), col("CustAccountBalance"))
      .withColumn("rank", rank().over(windowSpec))

    showDataFrameDetails("5 - rank per CustomerID", rank_df.orderBy("CustomerID", "rank").limit(100))

    // dense_rank.
    //
    // dense_rank() e' simile a rank, ma non lascia buchi.
    //
    // Esempio:
    //
    // saldo | dense_rank
    // 100   | 1
    // 100   | 1
    // 200   | 2
    //
    // Il valore 200 prende dense_rank 2, non 3.
    val dense_rank_df = df
      .select(col("CustomerID"), col("CustAccountBalance"))
      .withColumn("dense_rank", dense_rank().over(windowSpec))

    showDataFrameDetails("6 - dense_rank per CustomerID", dense_rank_df.orderBy("CustomerID", "dense_rank").limit(100))

    // Confronto row_number, rank e dense_rank nella stessa tabella.
    //
    // Questo e' utile per vedere subito la differenza:
    // - row_number: numero progressivo sempre diverso;
    // - rank: stesso valore per pari merito, con buchi;
    // - dense_rank: stesso valore per pari merito, senza buchi.
    val ranking_df = df
      .select(col("CustomerID"), col("CustAccountBalance"))
      .withColumn("row_num", row_number().over(windowSpec))
      .withColumn("rank", rank().over(windowSpec))
      .withColumn("dense_rank", dense_rank().over(windowSpec))

    showDataFrameDetails("7 - Confronto row_number, rank e dense_rank", ranking_df.orderBy("CustomerID", "row_num").limit(100))

    // percent_rank.
    //
    // percent_rank() restituisce la posizione relativa della riga dentro la
    // finestra, come valore tra 0 e 1.
    //
    // Per il primo record della finestra di solito vale 0.0.
    // Per l'ultimo record tende a 1.0.
    //
    // Serve quando vogliamo capire la posizione percentuale di una riga dentro
    // il gruppo, non solo la posizione intera.
    val percent_rank_df = ranking_df
      .withColumn("percent_rank", percent_rank().over(windowSpec))

    showDataFrameDetails("8 - percent_rank per CustomerID", percent_rank_df.orderBy("CustomerID", "row_num").limit(100))

    // lag.
    //
    // lag(col, 1) prende il valore della riga precedente dentro la stessa
    // finestra.
    //
    // Esempio:
    //
    // CustAccountBalance | lag
    // 6520.75            | null
    // 16406.51           | 6520.75
    // 24911.32           | 16406.51
    //
    // La prima riga non ha una riga precedente, quindi lag restituisce null.
    val lag_df = percent_rank_df
      .withColumn("lag", lag(col("CustAccountBalance"), 1).over(windowSpec))

    showDataFrameDetails("9 - lag: saldo precedente nello stesso CustomerID", lag_df.orderBy("CustomerID", "row_num").limit(100))

    // lead.
    //
    // lead(col, 1) prende il valore della riga successiva dentro la stessa
    // finestra.
    //
    // Esempio:
    //
    // CustAccountBalance | lead
    // 6520.75            | 16406.51
    // 16406.51           | 24911.32
    // 24911.32           | null
    //
    // L'ultima riga non ha una riga successiva, quindi lead restituisce null.
    val lag_lead_df = lag_df
      .withColumn("lead", lead(col("CustAccountBalance"), 1).over(windowSpec))

    showDataFrameDetails("10 - lag e lead", lag_lead_df.orderBy("CustomerID", "row_num").limit(100))

    // Focus su un cliente.
    //
    // Nel video viene filtrato un CustomerID specifico per vedere bene le righe
    // della stessa finestra. Usiamo un cliente presente nel dataset di esempio.
    //
    // Qui la tabella e' piu leggibile perche' mostra solo le righe di C1119249.
    val customer_focus_df = lag_lead_df
      .where(col("CustomerID") === "C1119249")
      .orderBy(col("row_num"))

    showDataFrameDetails("11 - Focus su CustomerID = C1119249", customer_focus_df)

    // Esempio con piu colonne di contesto.
    //
    // La finestra resta la stessa, ma manteniamo anche TransactionID,
    // TransactionDate e CustLocation per capire da quale transazione arriva ogni
    // saldo.
    val final_window_df = df
      .select(
        col("TransactionID"),
        col("CustomerID"),
        col("CustLocation"),
        col("CustAccountBalance"),
        col("TransactionDate"),
        col("Transaction_Amount")
      )
      .withColumn("row_num", row_number().over(windowSpec))
      .withColumn("rank", rank().over(windowSpec))
      .withColumn("dense_rank", dense_rank().over(windowSpec))
      .withColumn("percent_rank", percent_rank().over(windowSpec))
      .withColumn("lag", lag(col("CustAccountBalance"), 1).over(windowSpec))
      .withColumn("lead", lead(col("CustAccountBalance"), 1).over(windowSpec))
      .where(col("CustomerID") === "C1119249")
      .orderBy(col("row_num"))

    showDataFrameDetails("12 - Riepilogo Window Functions su un cliente", final_window_df)

    printSection("FINE - Job completato")
    println("Lo script ha mostrato Window.partitionBy, orderBy, row_number, rank, dense_rank, percent_rank, lag e lead.")

    spark.stop()
  }
}
