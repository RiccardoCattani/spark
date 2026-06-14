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
//
// Esempio globale
// ---------------
// Dataset iniziale:
// TransactionID | CustomerID | TransactionDate | Transaction_Amount
// T1            | C5841053   | 2/8/16          | 25.0
// T2            | C2142763   | 2/8/16          | 27999.0
//
// Dopo aver creato duplicati con union:
// TransactionID | CustomerID | TransactionDate | Transaction_Amount
// T1            | C5841053   | 2/8/16          | 25.0
// T2            | C2142763   | 2/8/16          | 27999.0
// T1            | C5841053   | 2/8/16          | 25.0
//
// Dopo distinct() o dropDuplicates():
// TransactionID | CustomerID | TransactionDate | Transaction_Amount
// T1            | C5841053   | 2/8/16          | 25.0
// T2            | C2142763   | 2/8/16          | 27999.0

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

    // Questa funzione stampa sempre lo stesso tipo di riepilogo per ogni DataFrame.
    //
    // Esempio output:
    //
    // Numero colonne: 5
    // Colonne: TransactionID, CustomerID, CustGender, TransactionDate, Transaction_Amount
    // Numero righe: 13
    // Schema:
    //  |-- TransactionID: string
    //  |-- Transaction_Amount: double
    // Dati mostrati: 13 righe su 13
    //
    // Serve per confrontare velocemente il "prima" e il "dopo" di ogni passaggio.

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

    // Stampa un conteggio compatto.
    //
    // Esempio output:
    // Righe selected_df                             10
    // Righe duplicate aggiunte                      3
    // Righe with_duplicates_df                      13
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

    // Lettura del CSV originale.
    //
    // Prima, file fisico:
    //
    // TransactionID,CustomerID,CustomerDOB,CustGender,CustLocation,CustAccountBalance,TransactionDate,TransactionTime,TransactionAmount (INR)
    // T1,C5841053,10/1/94,F,JAMSHEDPUR,17819.05,2/8/16,143207,25
    //
    // Dopo read.csv:
    //
    // TransactionID | CustomerID | CustomerDOB | CustGender | CustLocation | CustAccountBalance | TransactionDate | TransactionTime | TransactionAmount (INR)
    // T1            | C5841053   | 10/1/94     | F          | JAMSHEDPUR   | 17819.05           | 2/8/16          | 143207          | 25.0
    //
    // header=true usa la prima riga come nomi colonne.
    // inferSchema=true converte importi e numeri nei tipi Spark piu adatti.

    val read_csv_df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(BankTransactionsPath)
      .persist()

    showDataFrameDetails("1 - CSV originale", read_csv_df.limit(10))

    // Output atteso di questa sezione:
    //
    // - 9 colonne, cioe' tutte le colonne originali del CSV;
    // - 10 righe mostrate perche' usiamo limit(10);
    // - TransactionAmount (INR) viene letto come double grazie a inferSchema.

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
    //
    // Altro esempio:
    //
    // Prima:
    // TransactionID | CustomerID | CustomerDOB | CustGender | CustLocation | TransactionTime | TransactionAmount (INR)
    // T2            | C2142763   | 4/4/57      | M          | JHAJJAR      | 141858          | 27999.0
    //
    // Dopo:
    // TransactionID | CustomerID | CustGender | TransactionDate | Transaction_Amount
    // T2            | C2142763   | M          | 2/8/16          | 27999.0
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

    // Output atteso di questa sezione:
    //
    // - 5 colonne invece delle 9 originali;
    // - 10 righe;
    // - la colonna TransactionAmount (INR) non esiste piu' con quel nome:
    //   ora si chiama Transaction_Amount.

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
    //
    // Esempio piu completo:
    //
    // selected_df:
    // TransactionID | CustomerID | Transaction_Amount
    // T1            | C5841053   | 25.0
    // T2            | C2142763   | 27999.0
    // T3            | C4417068   | 459.0
    //
    // duplicated_rows_df:
    // TransactionID | CustomerID | Transaction_Amount
    // T1            | C5841053   | 25.0
    // T2            | C2142763   | 27999.0
    // T3            | C4417068   | 459.0
    //
    // with_duplicates_df:
    // contiene le 10 righe iniziali + T1, T2 e T3 ripetute.
    //
    // Conteggio atteso:
    // selected_df = 10 righe
    // duplicated_rows_df = 3 righe
    // with_duplicates_df = 13 righe

    val duplicated_rows_df = selected_df.limit(3)
    val with_duplicates_df = selected_df
      .union(duplicated_rows_df)
      .persist()

    showDataFrameDetails("3 - DataFrame con duplicati creati tramite union", with_duplicates_df)
    
    // Output atteso di questa sezione:
    //
    // Le righe T1, T2 e T3 compaiono due volte:
    //
    // T1 ... 25.0
    // T2 ... 27999.0
    // T3 ... 459.0
    // ...
    // T1 ... 25.0
    // T2 ... 27999.0
    // T3 ... 459.0
    //
    // Il conteggio totale passa da 10 a 13.

    printSection("4 - Conteggi prima della deduplica")

    // Risultato atteso:
    // Righe selected_df                             10
    // Righe duplicate aggiunte                      3
    // Righe with_duplicates_df                      13
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
    //
    // Altro esempio:
    //
    // Prima:
    // T2 | C2142763 | M | 2/8/16 | 27999.0
    // T2 | C2142763 | M | 2/8/16 | 27999.0
    //
    // Dopo:
    // T2 | C2142763 | M | 2/8/16 | 27999.0
    //
    // Nota: distinct puo' cambiare l'ordine delle righe mostrate, perche'
    // Spark distribuisce e riorganizza i dati durante la deduplica.
    
    val distinct_df = with_duplicates_df.distinct().persist()

    showDataFrameDetails("5 - Rimozione duplicati con distinct", distinct_df)
    // Output atteso di questa sezione:
    //
    // - le righe tornano da 13 a 10;
    // - T1, T2 e T3 restano una sola volta;
    // - l'ordine a video puo' essere diverso rispetto all'input.

    // dropDuplicates() senza parametri ha lo stesso effetto pratico di distinct:
    // rimuove duplicati considerando tutta la riga.
    //
    // Prima:
    // TransactionID | CustomerID | CustGender | TransactionDate | Transaction_Amount
    // T3            | C4417068   | F          | 2/8/16          | 459.0
    // T3            | C4417068   | F          | 2/8/16          | 459.0
    //
    // Dopo:
    // TransactionID | CustomerID | CustGender | TransactionDate | Transaction_Amount
    // T3            | C4417068   | F          | 2/8/16          | 459.0
    //
    // Differenza pratica:
    // - distinct() e' piu breve da scrivere;
    // - dropDuplicates() e' piu flessibile, perche' puo' ricevere una lista di colonne.
    
    val drop_duplicates_all_columns_df = with_duplicates_df.dropDuplicates().persist()

    showDataFrameDetails("6 - Rimozione duplicati con dropDuplicates su tutta la riga", drop_duplicates_all_columns_df)
    // Output atteso di questa sezione:
    // Stesso numero righe di distinct_df:
    // Con duplicati: 13
    // Dopo dropDuplicates(): 10
    // Perche' qui dropDuplicates() considera tutte le colonne della riga.

    printSection("7 - Confronto conteggi")
    
    // Risultato atteso:
    //
    // Con duplicati                                  13
    // Dopo distinct                                  10
    // Dopo dropDuplicates()                          10
    //
    // Se distinct e dropDuplicates() sono applicati a tutta la riga, il conteggio
    // finale deve coincidere.
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
    //
    // Questo esempio crea apposta due duplicati logici:
    //
    // Riga originale:
    // TransactionID | CustomerID | TransactionDate | Transaction_Amount
    // T1            | C5841053   | 2/8/16          | 25.0
    //
    // Riga modificata:
    // TransactionID | CustomerID | TransactionDate | Transaction_Amount
    // T1_COPY       | C5841053   | 2/8/16          | 1025.0
    //
    // Sono diverse se guardiamo tutta la riga, perche' TransactionID e importo
    // cambiano. Pero' sono duplicate se la regola di business dice:
    // "una transazione per CustomerID e TransactionDate".
    //
    // Dopo dropDuplicates(Seq("CustomerID", "TransactionDate")):
    // resta una sola tra T1 e T1_COPY.
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
    // Output atteso di questa sezione:
    //
    // Il DataFrame contiene 12 righe:
    // - le 10 righe originali di selected_df;
    // - 2 righe copiate, ma modificate in TransactionID e Transaction_Amount.
    //
    // Esempio:
    //
    // T1      | C5841053 | 2/8/16 | 25.0
    // T1_COPY | C5841053 | 2/8/16 | 1025.0
    //
    // Queste due righe NON sono duplicate complete, ma sono duplicate rispetto
    // alla coppia CustomerID + TransactionDate.

    val dedup_by_customer_day_df = duplicates_by_customer_day_df
      .dropDuplicates(Seq("CustomerID", "TransactionDate"))
      .persist()

    // Esempio del risultato della deduplica su colonne chiave:
    //
    // Prima:
    // TransactionID | CustomerID | TransactionDate | Transaction_Amount
    // T1            | C5841053   | 2/8/16          | 25.0
    // T1_COPY       | C5841053   | 2/8/16          | 1025.0
    //
    // Dopo:
    // TransactionID | CustomerID | TransactionDate | Transaction_Amount
    // T1            | C5841053   | 2/8/16          | 25.0
    //
    // Attenzione: Spark puo' tenere T1 oppure T1_COPY. Senza ordinamento non
    // bisogna basarsi su quale delle due venga mantenuta.

    showDataFrameDetails(
      "9 - dropDuplicates su CustomerID e TransactionDate",
      dedup_by_customer_day_df
    )
    // Output atteso di questa sezione:
    //
    // Il DataFrame torna da 12 a 10 righe.
    //
    // Per CustomerID=C5841053 e TransactionDate=2/8/16 rimane una sola riga:
    // T1 oppure T1_COPY.
    //
    // Per CustomerID=C2142763 e TransactionDate=2/8/16 rimane una sola riga:
    // T2 oppure T2_COPY.

    printSection("10 - Conteggi deduplica su colonne chiave")
    // Risultato atteso:
    //
    // Prima                                          12
    // Dopo dropDuplicates(CustomerID, TransactionDate) 10
    //
    // Questo dimostra che dropDuplicates su colonne chiave puo' eliminare righe
    // anche quando non sono identiche in tutte le colonne.
    printCount("Prima", duplicates_by_customer_day_df)
    printCount("Dopo dropDuplicates(CustomerID, TransactionDate)", dedup_by_customer_day_df)

    printSection("FINE - Job completato")
    println("Lo script ha mostrato distinct, dropDuplicates() e dropDuplicates su colonne specifiche.")
    println("Lettura del risultato: 13 -> 10 rimuove duplicati completi; 12 -> 10 rimuove duplicati logici su chiavi.")

    spark.stop()
  }
}
