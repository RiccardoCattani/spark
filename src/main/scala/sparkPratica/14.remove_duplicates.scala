package sparkPratica

// sbt "runMain sparkPratica.obj_remove_duplicates"
//
// Scopo dello script
// ------------------
// Questo script mostra come rimuovere duplicati da un DataFrame Spark.
//
// In particolare dimostra:
// - come leggere il CSV bank_transactions.csv;
// - come creare un piccolo DataFrame di esempio con duplicati controllati;
// - come usare distinct() per rimuovere righe completamente duplicate;
// - come usare dropDuplicates() per ottenere lo stesso risultato sulle righe intere;
// - come usare dropDuplicates(Seq(...)) per rimuovere duplicati guardando solo
//   alcune colonne chiave, per esempio CustomerID e TransactionDate.
//
// Differenza importante
// ---------------------
// distinct() e dropDuplicates() senza colonne lavorano su tutta la riga.
// Due righe sono duplicate solo se tutti i valori di tutte le colonne sono uguali.
//
// dropDuplicates(Seq("colonna1", "colonna2")) invece guarda solo le colonne
// indicate. Se due righe hanno gli stessi valori in quelle colonne, Spark ne
// mantiene una sola anche se le altre colonne sono diverse.

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object obj_remove_duplicates {
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

  private def printCount(label: String, df: DataFrame): Unit = {
    println(f"$label%-45s ${df.count()}%,d")
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("remove-duplicates")
      .setMaster("local[*]")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    printSection("AVVIO - Rimozione duplicati")
    println("Leggo il CSV delle transazioni bancarie e preparo un esempio piccolo con duplicati.")

    val read_csv_df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(BankTransactionsPath)
      .persist()

    showDataFrameDetails("1 - CSV originale", read_csv_df.limit(10))

    // Per l'esempio selezioniamo poche colonne.
    //
    // Prima:
    //
    // TransactionID | CustomerID | CustGender | TransactionDate | TransactionAmount (INR)
    // T1            | C5841053   | F          | 2/8/16          | 25.0
    //
    // Dopo la select:
    //
    // TransactionID | CustomerID | CustGender | TransactionDate | Transaction_Amount
    // T1            | C5841053   | F          | 2/8/16          | 25.0
    val selected_df = read_csv_df
      .select(
        col("TransactionID"),
        col("CustomerID"),
        col("CustGender"),
        col("TransactionDate"),
        col("TransactionAmount (INR)").alias("Transaction_Amount")
      )
      .limit(10)
      .persist()

    showDataFrameDetails("2 - DataFrame piccolo senza duplicati creati da noi", selected_df)

    // Creiamo duplicati controllati.
    //
    // Il file originale puo' avere o non avere duplicati identici. Per rendere
    // l'esempio sempre visibile, prendiamo le prime 3 righe e le aggiungiamo di
    // nuovo al DataFrame con union.
    //
    // union concatena due DataFrame con lo stesso schema e NON rimuove duplicati.
    //
    // Esempio logico:
    //
    // Prima:
    // T1
    // T2
    // T3
    //
    // Dopo union con T1, T2, T3:
    // T1
    // T2
    // T3
    // ...
    // T1
    // T2
    // T3
    val duplicated_rows_df = selected_df.limit(3)
    val with_duplicates_df = selected_df
      .union(duplicated_rows_df)
      .persist()

    showDataFrameDetails("3 - DataFrame con duplicati creati tramite union", with_duplicates_df)

    printSection("4 - Conteggi prima della deduplica")
    printCount("Righe selected_df", selected_df)
    printCount("Righe duplicate aggiunte", duplicated_rows_df)
    printCount("Righe with_duplicates_df", with_duplicates_df)

    // distinct rimuove le righe duplicate guardando tutte le colonne.
    //
    // Se due righe sono identiche in tutte le colonne, ne resta una sola.
    //
    // Prima:
    //
    // TransactionID | CustomerID | CustGender | TransactionDate | Transaction_Amount
    // T1            | C5841053   | F          | 2/8/16          | 25.0
    // T1            | C5841053   | F          | 2/8/16          | 25.0
    //
    // Dopo distinct:
    //
    // TransactionID | CustomerID | CustGender | TransactionDate | Transaction_Amount
    // T1            | C5841053   | F          | 2/8/16          | 25.0
    val distinct_df = with_duplicates_df.distinct().persist()

    showDataFrameDetails("5 - Rimozione duplicati con distinct", distinct_df)

    // dropDuplicates() senza parametri ha lo stesso effetto pratico di distinct:
    // rimuove duplicati considerando tutta la riga.
    val drop_duplicates_all_columns_df = with_duplicates_df.dropDuplicates().persist()

    showDataFrameDetails("6 - Rimozione duplicati con dropDuplicates su tutta la riga", drop_duplicates_all_columns_df)

    printSection("7 - Confronto conteggi")
    printCount("Con duplicati", with_duplicates_df)
    printCount("Dopo distinct", distinct_df)
    printCount("Dopo dropDuplicates()", drop_duplicates_all_columns_df)

    // dropDuplicates su un sottoinsieme di colonne.
    //
    // Qui Spark guarda solo CustomerID e TransactionDate.
    // Se due righe hanno stesso cliente e stessa data, ne mantiene una sola.
    // Le altre colonne non vengono usate per decidere se le righe sono duplicate.
    //
    // Esempio:
    //
    // CustomerID | TransactionDate | TransactionID | Transaction_Amount
    // C5841053   | 2/8/16          | T1            | 25.0
    // C5841053   | 2/8/16          | T999          | 100.0
    //
    // Dopo dropDuplicates(Seq("CustomerID", "TransactionDate")) resta una sola
    // delle due righe. Spark non garantisce quale riga tenga, se non specifichiamo
    // una regola di ordinamento.
    val duplicates_by_customer_day_df = selected_df
      .union(
        selected_df
          .limit(2)
          .withColumn("TransactionID", concat(col("TransactionID"), lit("_COPY")))
          .withColumn("Transaction_Amount", col("Transaction_Amount") + lit(1000.0))
      )
      .persist()

    showDataFrameDetails(
      "8 - Duplicati logici: stesso CustomerID e TransactionDate ma altri valori diversi",
      duplicates_by_customer_day_df
    )

    val dedup_by_customer_day_df = duplicates_by_customer_day_df
      .dropDuplicates(Seq("CustomerID", "TransactionDate"))
      .persist()

    showDataFrameDetails(
      "9 - dropDuplicates su CustomerID e TransactionDate",
      dedup_by_customer_day_df
    )

    printSection("10 - Conteggi deduplica su colonne chiave")
    printCount("Prima", duplicates_by_customer_day_df)
    printCount("Dopo dropDuplicates(CustomerID, TransactionDate)", dedup_by_customer_day_df)

    printSection("FINE - Job completato")
    println("Lo script ha mostrato distinct, dropDuplicates() e dropDuplicates su colonne specifiche.")

    spark.stop()
  }
}
