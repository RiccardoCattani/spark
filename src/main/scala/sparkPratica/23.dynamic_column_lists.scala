package sparkPratica

// sbt "runMain sparkPratica.obj_dynamic_column_lists"
//
// Scopo dello script
// ------------------
// Questo script continua l'argomento dello script 22:
//
// passare liste di colonne a un DataFrame Spark.
//
// Lo script 22 mostrava il caso base:
//
// val colonne = List("CustomerID", "TransactionID", "CustAccountBalance")
// df.select(colonne.map(col): _*)
//
// Qui facciamo un passo in piu e vediamo come usare liste di colonne in modo
// piu pratico:
//
// - selezionare colonne richieste da una lista;
// - validare i nomi prima della select;
// - creare alias leggibili;
// - costruire colonne calcolate partendo da una configurazione;
// - riusare la stessa lista per drop dei null, ordinamento e report finale.
//
// Idea centrale:
//
// quando le colonne sono tante o cambiano spesso, conviene separare:
//
// 1. la configurazione
//    cioe' quali colonne vogliamo;
//
// 2. la logica Spark
//    cioe' come trasformiamo quei nomi in Column.
//
// In questo modo il codice diventa piu facile da modificare:
// cambiamo una lista, non riscriviamo ogni select.
//
// Esempio:
//
// se domani vogliamo aggiungere CustLocation al report, non dobbiamo cercare
// tutte le select nel codice. Basta aggiungere "CustLocation" alla lista.
//
// Prima:
//
// val colonne = Seq("CustomerID", "TransactionID")
//
// Dopo:
//
// val colonne = Seq("CustomerID", "TransactionID", "CustLocation")
//
// La select resta uguale:
//
// df.select(colonne.map(col): _*)

import org.apache.spark.SparkConf
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object obj_dynamic_column_lists {
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

  // Restituisce solo le colonne richieste che esistono davvero nel DataFrame.
  //
  // Esempio:
  //
  // df.columns
  //   = Array("CustomerID", "TransactionID", "CustLocation")
  //
  // requestedColumns
  //   = Seq("CustomerID", "CustLocation", "ColonnaNonPresente")
  //
  // Risultato:
  //   Seq("CustomerID", "CustLocation")
  //
  // Usiamo df.columns.toSet per rendere il controllo di presenza piu diretto:
  //
  // availableColumns.contains("CustomerID")          -> true
  // availableColumns.contains("ColonnaNonPresente") -> false
  private def existingColumns(df: DataFrame, requestedColumns: Seq[String]): Seq[String] = {
    val availableColumns = df.columns.toSet
    requestedColumns.filter(availableColumns.contains)
  }

  // Restituisce le colonne richieste che NON esistono nel DataFrame.
  //
  // Esempio:
  //
  // df.columns
  //   = Array("CustomerID", "TransactionID", "CustLocation")
  //
  // requestedColumns
  //   = Seq("CustomerID", "CustLocation", "ColonnaNonPresente")
  //
  // Risultato:
  //   Seq("ColonnaNonPresente")
  //
  // Questo controllo serve per stampare un messaggio chiaro prima di fare la
  // select, invece di lasciare che Spark fallisca con un errore piu lungo.
  private def missingColumns(df: DataFrame, requestedColumns: Seq[String]): Seq[String] = {
    val availableColumns = df.columns.toSet
    requestedColumns.filterNot(availableColumns.contains)
  }

  // Esegue una select protetta.
  //
  // Problema:
  //
  // df.select("CustomerID", "ColonnaNonPresente")
  //
  // genera errore, perche' ColonnaNonPresente non esiste.
  //
  // Soluzione:
  //
  // 1. filtro la lista con existingColumns;
  // 2. converto i nomi validi in Column con map(col);
  // 3. passo le colonne a select con : _*.
  //
  // Esempio equivalente:
  //
  // val validColumns = Seq("CustomerID", "TransactionID")
  // df.select(validColumns.map(col): _*)
  private def selectExistingColumns(df: DataFrame, requestedColumns: Seq[String]): DataFrame = {
    val validColumns = existingColumns(df, requestedColumns)
    df.select(validColumns.map(col): _*)
  }

  // Converte una configurazione di alias in una lista di Column Spark.
  //
  // Input:
  //
  // Seq(
  //   "CustomerID" -> "Customer",
  //   "TransactionID" -> "Transaction"
  // )
  //
  // Output concettuale:
  //
  // Seq(
  //   col("CustomerID").alias("Customer"),
  //   col("TransactionID").alias("Transaction")
  // )
  //
  // Questo permette di separare il "cosa voglio rinominare" dal codice Spark
  // che applica davvero gli alias.
  private def columnsWithAlias(aliasMap: Seq[(String, String)]): Seq[Column] = {
    aliasMap.map { case (sourceColumn, targetColumn) =>
      col(sourceColumn).alias(targetColumn)
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("dynamic-column-lists")
      .setMaster("local[*]")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    printSection("AVVIO - Liste di colonne dinamiche")
    println("Leggo bank_transactions.csv e mostro vari modi pratici per guidare select, alias e report con liste.")

    // Lettura del CSV originale.
    //
    // In questo file continuiamo a usare bank_transactions.csv, lo stesso del
    // video e dello script 22.
    //
    // Colonne principali del dataset:
    //
    // TransactionID
    // CustomerID
    // CustomerDOB
    // CustGender
    // CustLocation
    // CustAccountBalance
    // TransactionDate
    // TransactionTime
    // TransactionAmount (INR)
    //
    // inferSchema=true chiede a Spark di provare a leggere i tipi numerici.
    val read_csv_df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(BankTransactionsPath)
      .persist()

    showDataFrameDetails("1 - CSV originale limitato alle prime righe", read_csv_df.limit(10))

    // Lista base di colonne per un report.
    //
    // Questa e' una configurazione semplice: un elenco di nomi colonna che
    // vogliamo portare nel DataFrame finale.
    //
    // Nota:
    // TransactionAmount (INR) contiene spazio e parentesi nel nome.
    // Con col("TransactionAmount (INR)") Spark la gestisce correttamente.
    //
    // Senza lista scriveremmo:
    //
    // read_csv_df.select(
    //   col("CustomerID"),
    //   col("TransactionID"),
    //   col("CustGender"),
    //   col("CustLocation"),
    //   col("CustAccountBalance"),
    //   col("TransactionAmount (INR)")
    // )
    //
    // Con lista scriviamo invece la configurazione una sola volta e la
    // convertiamo in Column piu sotto con reportColumns.map(col).
    val reportColumns = Seq(
      "CustomerID",
      "TransactionID",
      "CustGender",
      "CustLocation",
      "CustAccountBalance",
      "TransactionAmount (INR)"
    )

    printSection("2 - Configurazione: lista colonne report")
    println(s"Colonne richieste: ${reportColumns.mkString(", ")}")

    // reportColumns.map(col)
    //   trasforma Seq[String] in Seq[Column].
    //
    // Prima:
    //
    // Seq("CustomerID", "TransactionID")
    //
    // Dopo:
    //
    // Seq(col("CustomerID"), col("TransactionID"))
    //
    // : _*
    //   spacchetta la Seq[Column] e la passa a select come argomenti separati.
    //
    // Quindi questa istruzione equivale a:
    //
    // read_csv_df.select(
    //   col("CustomerID"),
    //   col("TransactionID"),
    //   col("CustGender"),
    //   col("CustLocation"),
    //   col("CustAccountBalance"),
    //   col("TransactionAmount (INR)")
    // )
    val report_df = read_csv_df.select(reportColumns.map(col): _*)

    showDataFrameDetails("3 - Report creato con reportColumns.map(col): _*", report_df.limit(100))

    // Validazione prima della select.
    //
    // In un programma reale la lista puo' arrivare da un file di configurazione,
    // da una UI o da input utente.
    //
    // Se la lista contiene colonne non presenti, Spark fallisce con un errore
    // di analisi. Qui controlliamo prima cosa e' valido e cosa manca.
    //
    // Esempio di caso rischioso:
    //
    // val requestedByUser = Seq("CustomerID", "ColonnaNonPresente")
    // read_csv_df.select(requestedByUser.map(col): _*)
    //
    // Spark cerca ColonnaNonPresente nello schema e non la trova.
    //
    // In questo script, invece, facciamo:
    //
    // validRequestedColumns
    //   contiene solo le colonne usabili.
    //
    // missingRequestedColumns
    //   contiene le colonne da segnalare.
    val requestedByUser = Seq(
      "CustomerID",
      "TransactionID",
      "CustLocation",
      "TransactionAmount (INR)",
      "ColonnaNonPresente"
    )

    val validRequestedColumns = existingColumns(read_csv_df, requestedByUser)
    val missingRequestedColumns = missingColumns(read_csv_df, requestedByUser)

    printSection("4 - Validazione colonne richieste")
    println(s"Richieste: ${requestedByUser.mkString(", ")}")
    println(s"Valide: ${validRequestedColumns.mkString(", ")}")
    println(s"Mancanti: ${missingRequestedColumns.mkString(", ")}")

    // selectExistingColumns applica la regola:
    //
    // richieste dall'utente:
    //   CustomerID, TransactionID, CustLocation, TransactionAmount (INR),
    //   ColonnaNonPresente
    //
    // presenti nel DataFrame:
    //   CustomerID, TransactionID, CustLocation, TransactionAmount (INR)
    //
    // select finale:
    //   solo le 4 colonne presenti.
    val safe_report_df = selectExistingColumns(read_csv_df, requestedByUser)

    showDataFrameDetails("5 - Select solo con colonne esistenti", safe_report_df.limit(100))

    // Alias guidati da una lista di coppie.
    //
    // Finora la lista conteneva solo String.
    // Ora usiamo una lista di coppie:
    //
    // (colonnaOrigine, colonnaDestinazione)
    //
    // Esempio:
    //
    // "CustomerID" -> "Customer"
    //
    // columnsWithAlias trasforma ogni coppia in:
    //
    // col("CustomerID").alias("Customer")
    //
    // Esempio completo:
    //
    // aliasConfig:
    //   "CustomerID" -> "Customer"
    //   "TransactionID" -> "Transaction"
    //
    // diventa:
    //   col("CustomerID").alias("Customer")
    //   col("TransactionID").alias("Transaction")
    //
    // Il DataFrame risultante non avra' piu le colonne CustomerID e
    // TransactionID, ma le colonne Customer e Transaction.
    val aliasConfig = Seq(
      "CustomerID" -> "Customer",
      "TransactionID" -> "Transaction",
      "CustGender" -> "Gender",
      "CustLocation" -> "Location",
      "CustAccountBalance" -> "Balance",
      "TransactionAmount (INR)" -> "AmountINR"
    )

    val aliased_columns = columnsWithAlias(aliasConfig)

    printSection("6 - Lista di alias")
    aliasConfig.foreach { case (sourceColumn, targetColumn) =>
      println(s"$sourceColumn -> $targetColumn")
    }

    // Qui aliased_columns e' gia' una Seq[Column].
    //
    // Per questo non serve map(col), perche' la conversione e' gia' stata fatta
    // dentro columnsWithAlias.
    //
    // Usiamo direttamente:
    //
    // read_csv_df.select(aliased_columns: _*)
    val aliased_report_df = read_csv_df.select(aliased_columns: _*)

    showDataFrameDetails("7 - Report con alias generati da lista di coppie", aliased_report_df.limit(100))

    // Colonne tecniche con cast e pulizia.
    //
    // Dopo aver rinominato le colonne, possiamo normalizzare i tipi:
    //
    // Balance
    //   saldo del conto, convertito a double.
    //
    // AmountINR
    //   importo della transazione, convertito a double.
    //
    // Location
    //   localita' pulita con trim e upper.
    //
    // Questo passaggio e' comune prima di aggregazioni e report.
    //
    // Esempio:
    //
    // Prima:
    //
    // Location = "  mumbai "
    // Balance = "17819.05"
    // AmountINR = "25.0"
    //
    // Dopo:
    //
    // Location = "MUMBAI"
    // Balance = 17819.05 come double
    // AmountINR = 25.0 come double
    //
    // Nel nostro CSV Spark ha gia' letto Balance e AmountINR come double grazie
    // a inferSchema=true, ma il cast resta utile come esempio di normalizzazione
    // e rende chiara l'intenzione del codice.
    val normalized_df = aliased_report_df
      .withColumn("Balance", col("Balance").cast("double"))
      .withColumn("AmountINR", col("AmountINR").cast("double"))
      .withColumn("Location", upper(trim(col("Location"))))
      .persist()

    showDataFrameDetails("8 - Report normalizzato: cast numerici e location maiuscola", normalized_df.limit(100))

    // Lista di colonne obbligatorie.
    //
    // Usiamo una lista anche per decidere dove i null non sono accettabili.
    //
    // In questo esempio una riga e' utile per il report solo se ha:
    //
    // Customer
    // Transaction
    // Location
    // Balance
    // AmountINR
    //
    // na.drop(requiredColumns)
    // elimina le righe che hanno null in almeno una delle colonne indicate.
    //
    // Esempio:
    //
    // Customer | Transaction | Location | Balance | AmountINR
    // C1       | T1          | MUMBAI   | 100.0   | 25.0
    // C2       | T2          | DELHI    | null    | 40.0
    //
    // Con:
    //
    // na.drop(Seq("Customer", "Transaction", "Location", "Balance", "AmountINR"))
    //
    // la riga C1 resta, la riga C2 viene rimossa perche' Balance e' null.
    val requiredColumns = Seq("Customer", "Transaction", "Location", "Balance", "AmountINR")

    val cleaned_df = normalized_df
      .na.drop(requiredColumns)
      .persist()

    printSection("9 - Colonne obbligatorie")
    println(s"Colonne usate per na.drop: ${requiredColumns.mkString(", ")}")

    showDataFrameDetails("10 - Report dopo rimozione dei null nelle colonne obbligatorie", cleaned_df.limit(100))

    // Lista di metriche calcolate.
    //
    // Qui costruiamo una Seq[Column] direttamente, non una Seq[String].
    //
    // Questo serve quando una colonna non e' solo un riferimento a un campo,
    // ma una vera espressione Spark:
    //
    // round(col("AmountINR"), 2)
    // when(...).otherwise(...)
    // concat_ws(...)
    //
    // Poi passiamo tutta la lista a select con : _*.
    //
    // Esempio di differenza tra Seq[String] e Seq[Column]:
    //
    // Seq[String]:
    //   Seq("Customer", "Transaction")
    //   indica solo nomi di colonne.
    //
    // Seq[Column]:
    //   Seq(
    //     col("Customer"),
    //     round(col("AmountINR"), 2).alias("AmountRounded"),
    //     when(col("AmountINR") >= 10000, lit("HIGH")).otherwise(lit("LOW"))
    //   )
    //
    // permette di mettere nella select anche espressioni, arrotondamenti,
    // condizioni e alias.
    //
    // AmountBand nell'esempio sotto funziona cosi':
    //
    // AmountINR >= 10000  -> HIGH
    // AmountINR >= 1000   -> MEDIUM
    // altri valori        -> LOW
    val calculatedColumns = Seq(
      col("Customer"),
      col("Transaction"),
      col("Gender"),
      col("Location"),
      round(col("Balance"), 2).alias("BalanceRounded"),
      round(col("AmountINR"), 2).alias("AmountRounded"),
      when(col("AmountINR") >= 10000, lit("HIGH"))
        .when(col("AmountINR") >= 1000, lit("MEDIUM"))
        .otherwise(lit("LOW"))
        .alias("AmountBand"),
      concat_ws(" - ", col("Customer"), col("Transaction")).alias("CustomerTransactionKey")
    )

    val calculated_report_df = cleaned_df.select(calculatedColumns: _*)

    showDataFrameDetails("11 - Select con lista di Column calcolate", calculated_report_df.limit(100))

    // Ordinamento guidato da configurazione.
    //
    // Anche l'orderBy puo' essere costruito con una lista di Column.
    //
    // Qui ordiniamo per:
    //
    // 1. AmountRounded decrescente;
    // 2. BalanceRounded decrescente;
    // 3. Customer crescente.
    //
    // Forma scritta a mano:
    //
    // calculated_report_df.orderBy(
    //   col("AmountRounded").desc,
    //   col("BalanceRounded").desc,
    //   col("Customer").asc
    // )
    //
    // Forma guidata da lista:
    //
    // val orderingColumns = Seq(...)
    // calculated_report_df.orderBy(orderingColumns: _*)
    //
    // Il vantaggio e' che possiamo costruire orderingColumns in modo dinamico,
    // per esempio aggiungendo una colonna di ordinamento solo se serve.
    val orderingColumns = Seq(
      col("AmountRounded").desc,
      col("BalanceRounded").desc,
      col("Customer").asc
    )

    val ordered_report_df = calculated_report_df.orderBy(orderingColumns: _*)

    showDataFrameDetails("12 - Report ordinato con Seq[Column]", ordered_report_df.limit(100))

    // Aggregazione finale guidata da liste.
    //
    // Prima scegliamo quali localita' includere.
    // Poi usiamo una lista di metriche aggregate.
    //
    // Questo pattern e' utile quando un report ha gruppi e metriche
    // configurabili.
    //
    // selectedLocations limita il report a poche localita' importanti:
    //
    // Seq("MUMBAI", "NEW DELHI", "BANGALORE", "GURGAON", "DELHI")
    //
    // Senza questo filtro, l'aggregazione produrrebbe una riga per ogni
    // localita' distinta del dataset.
    val selectedLocations = Seq("MUMBAI", "NEW DELHI", "BANGALORE", "GURGAON", "DELHI")

    // aggregationMetrics e' una lista di espressioni aggregate.
    //
    // Forma scritta a mano:
    //
    // .agg(
    //   count(lit(1)).alias("Transactions"),
    //   round(sum(col("AmountINR")), 2).alias("TotalAmount"),
    //   round(avg(col("AmountINR")), 2).alias("AvgAmount")
    // )
    //
    // Forma guidata da lista:
    //
    // val aggregationMetrics = Seq(...)
    // .agg(aggregationMetrics.head, aggregationMetrics.tail: _*)
    //
    // Nota su agg:
    //
    // agg richiede almeno una Column obbligatoria come primo parametro.
    // Per questo passiamo:
    //
    // aggregationMetrics.head
    //   prima metrica.
    //
    // aggregationMetrics.tail: _*
    //   tutte le altre metriche spacchettate come argomenti variabili.
    val aggregationMetrics = Seq(
      count(lit(1)).alias("Transactions"),
      round(sum(col("AmountINR")), 2).alias("TotalAmount"),
      round(avg(col("AmountINR")), 2).alias("AvgAmount"),
      round(max(col("AmountINR")), 2).alias("MaxAmount"),
      round(avg(col("Balance")), 2).alias("AvgBalance")
    )

    val location_summary_df = cleaned_df
      // isin(selectedLocations: _*) equivale a:
      //
      // Location IN ("MUMBAI", "NEW DELHI", "BANGALORE", "GURGAON", "DELHI")
      //
      // Anche qui : _* spacchetta la Seq[String].
      .where(col("Location").isin(selectedLocations: _*))
      .groupBy(col("Location"))
      // Applichiamo tutte le metriche definite nella lista aggregationMetrics.
      .agg(aggregationMetrics.head, aggregationMetrics.tail: _*)
      .orderBy(col("TotalAmount").desc)

    showDataFrameDetails("13 - Aggregazione finale con metriche da lista", location_summary_df)

    printSection("FINE - Job completato")
    println("Lo script ha mostrato liste per select, validazione, alias, cast, drop dei null, ordinamento e aggregazioni.")

    spark.stop()
  }
}
