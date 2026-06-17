package sparkPratica

// sbt "runMain sparkPratica.obj_pass_list_to_dataframe"
//
// Scopo dello script
// ------------------
// Questo script mostra come passare una lista di nomi colonna a un DataFrame
// Spark per selezionare colonne in modo dinamico.
//
// Il video di riferimento usa il file bank_transactions.csv e mostra:
//
// - lettura del CSV
// - select("CustomerID", "TransactionID", "CustAccountBalance", "CustGender")
// - creazione di una List con gli stessi nomi colonna
// - conversione della List in colonne Spark con list.map(col)
// - passaggio della lista a select con : _*
//
// L'idea centrale e':
//
// invece di scrivere:
//
// df.select("CustomerID", "TransactionID", "CustAccountBalance", "CustGender")
//
// possiamo scrivere:
//
// val colonne = List("CustomerID", "TransactionID", "CustAccountBalance", "CustGender")
// df.select(colonne.map(col): _*)
//
// Questo e' utile quando le colonne non sono fisse nel codice, ma arrivano da:
//
// - una lista costruita nel programma;
// - una configurazione;
// - una scelta dell'utente;
// - una logica che aggiunge o rimuove colonne in base a condizioni.
//
// Punto importante:
//
// select in Spark accetta colonne come argomenti separati:
//
// select(col("A"), col("B"), col("C"))
//
// Ma colonne.map(col) produce una lista:
//
// List(col("A"), col("B"), col("C"))
//
// La sintassi : _* serve proprio a "spacchettare" la lista e passarla a select
// come se fossero argomenti separati.

import org.apache.spark.SparkConf
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object obj_pass_list_to_dataframe {
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

  private def selectColumnsByName(df: DataFrame, columnNames: Seq[String]): DataFrame = {
    df.select(columnNames.map(col): _*)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("pass-list-to-dataframe")
      .setMaster("local[*]")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    printSection("AVVIO - Passare una lista di colonne a un DataFrame")
    println("Leggo bank_transactions.csv e mostro come usare List(...).map(col): _* dentro select.")

    // Lettura del CSV originale.
    //
    // Prima, file fisico:
    //
    // TransactionID,CustomerID,CustomerDOB,CustGender,CustLocation,CustAccountBalance,TransactionDate,TransactionTime,TransactionAmount (INR)
    // T1,C5841053,10/1/94,F,JAMSHEDPUR,17819.05,2/8/16,143207,25
    //
    // Dopo read.csv:
    //
    // Spark crea un DataFrame con una colonna per ogni campo del CSV.
    //
    // Usiamo inferSchema=true per far leggere a Spark i tipi quando possibile:
    //
    // CustAccountBalance
    //   diventa double.
    //
    // TransactionTime
    //   diventa integer.
    //
    // TransactionAmount (INR)
    //   diventa double.
    val read_csv_df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(BankTransactionsPath)
      .persist()

    showDataFrameDetails("1 - CSV originale limitato alle prime righe", read_csv_df.limit(10))

    // Esempio classico: select con nomi colonna scritti direttamente.
    //
    // Questo e' il modo piu semplice quando sappiamo gia' quali colonne
    // vogliamo e non abbiamo bisogno di costruire la lista in modo dinamico.
    //
    // select("CustomerID", "TransactionID", ...)
    //   accetta una sequenza di stringhe, ognuna corrispondente al nome di una
    //   colonna del DataFrame.
    //
    // Limite:
    // se la lista delle colonne cambia spesso, dobbiamo modificare il codice
    // ogni volta.
    val direct_select_df = read_csv_df.select(
      "CustomerID",
      "TransactionID",
      "CustAccountBalance",
      "CustGender"
    )

    showDataFrameDetails("2 - Select diretto con nomi colonna", direct_select_df.limit(100))

    // Esempio del video: mettiamo i nomi colonna dentro una List.
    //
    // Questa lista contiene stringhe, non colonne Spark.
    //
    // Tipo logico:
    //
    // List[String]
    //
    // Contenuto:
    //
    // "CustomerID"
    // "TransactionID"
    // "CustAccountBalance"
    // "CustGender"
    //
    // Da sola, questa lista e' solo un elenco di nomi.
    // Per usarla in select dobbiamo trasformare ogni stringa in una Column.
    val list1 = List(
      "CustomerID",
      "TransactionID",
      "CustAccountBalance",
      "CustGender"
    )

    printSection("3 - Lista di nomi colonna")
    println(s"Lista originale: ${list1.mkString(", ")}")
    println("Tipo concettuale: List[String]")

    // Conversione da List[String] a Seq[Column].
    //
    // col("CustomerID")
    //   crea una Column Spark che fa riferimento alla colonna CustomerID.
    //
    // list1.map(col)
    //   applica col(...) a ogni elemento della lista.
    //
    // Prima:
    //
    // List("CustomerID", "TransactionID")
    //
    // Dopo:
    //
    // List(col("CustomerID"), col("TransactionID"))
    //
    // Tipo concettuale:
    //
    // Seq[Column]
    val column_objects: Seq[Column] = list1.map(col)

    printSection("4 - Conversione della lista in Column Spark")
    println("list1.map(col) trasforma ogni nome colonna in un oggetto Column.")
    println(s"Numero colonne convertite: ${column_objects.length}")

    // Passaggio importante: : _*
    //
    // select non vuole una singola lista di Column.
    // select vuole argomenti separati.
    //
    // Questo NON e' il formato atteso:
    //
    // select(List(col("CustomerID"), col("TransactionID")))
    //
    // Questo invece e' il formato atteso:
    //
    // select(col("CustomerID"), col("TransactionID"))
    //
    // La sintassi : _* dice a Scala:
    //
    // "Prendi questa Seq[Column] e passala come argomenti variabili."
    //
    // Quindi:
    //
    // read_csv_df.select(list1.map(col): _*)
    //
    // equivale a:
    //
    // read_csv_df.select(
    //   col("CustomerID"),
    //   col("TransactionID"),
    //   col("CustAccountBalance"),
    //   col("CustGender")
    // )
    val df2 = read_csv_df.select(list1.map(col): _*)

    showDataFrameDetails("5 - Select usando List(...).map(col): _*", df2.limit(100))

    // Stesso risultato passando da una funzione helper.
    //
    // Questo pattern e' utile quando vogliamo riutilizzare la logica in piu
    // punti del progetto.
    //
    // La funzione selectColumnsByName riceve:
    //
    // df
    //   il DataFrame di partenza.
    //
    // columnNames
    //   una Seq[String] con i nomi da selezionare.
    //
    // E restituisce:
    //
    // un nuovo DataFrame con solo quelle colonne.
    val selected_with_helper_df = selectColumnsByName(read_csv_df, list1)

    showDataFrameDetails("6 - Select dinamico tramite funzione helper", selected_with_helper_df.limit(100))

    // Costruzione dinamica della lista.
    //
    // In un caso reale potremmo voler partire da una lista base e aggiungere
    // colonne solo se servono.
    //
    // Qui simuliamo una piccola logica:
    //
    // - teniamo sempre CustomerID e TransactionID;
    // - aggiungiamo CustLocation se includeLocation=true;
    // - aggiungiamo TransactionAmount (INR) se includeAmount=true.
    val includeLocation = true
    val includeAmount = true

    val base_columns = List("CustomerID", "TransactionID")
    val location_columns = if (includeLocation) List("CustLocation") else List.empty[String]
    val amount_columns = if (includeAmount) List("TransactionAmount (INR)") else List.empty[String]

    val dynamic_columns = base_columns ++ location_columns ++ amount_columns

    printSection("7 - Lista costruita dinamicamente")
    println(s"Colonne base: ${base_columns.mkString(", ")}")
    println(s"Colonne finali: ${dynamic_columns.mkString(", ")}")

    val dynamic_select_df = selectColumnsByName(read_csv_df, dynamic_columns)

    showDataFrameDetails("8 - Select con lista costruita dinamicamente", dynamic_select_df.limit(100))

    // Validazione delle colonne prima della select.
    //
    // Se passiamo a select un nome colonna inesistente, Spark genera un errore
    // di analisi.
    //
    // Esempio:
    //
    // df.select("CustomerID", "ColonnaCheNonEsiste")
    //
    // Per evitare errori poco chiari, possiamo controllare prima quali colonne
    // richieste sono presenti nel DataFrame.
    val requested_columns = List(
      "CustomerID",
      "TransactionID",
      "CustAccountBalance",
      "ColonnaCheNonEsiste"
    )

    val available_columns = read_csv_df.columns.toSet
    val valid_columns = requested_columns.filter(available_columns.contains)
    val missing_columns = requested_columns.filterNot(available_columns.contains)

    printSection("9 - Validazione dei nomi colonna")
    println(s"Colonne richieste: ${requested_columns.mkString(", ")}")
    println(s"Colonne valide: ${valid_columns.mkString(", ")}")
    println(s"Colonne mancanti: ${missing_columns.mkString(", ")}")

    val safe_select_df = selectColumnsByName(read_csv_df, valid_columns)

    showDataFrameDetails("10 - Select solo con colonne valide", safe_select_df.limit(100))

    // Lista di Column con trasformazioni.
    //
    // Finora la lista conteneva solo nomi colonna.
    // Possiamo anche costruire direttamente una Seq[Column] con espressioni,
    // alias e cast.
    //
    // Questo e' utile quando vogliamo selezionare colonne e, nello stesso tempo:
    //
    // - rinominarle;
    // - convertire il tipo;
    // - applicare funzioni Spark.
    //
    // Esempio:
    //
    // col("CustAccountBalance").cast("double").alias("Balance")
    //
    // prende CustAccountBalance, la converte a double e la rinomina Balance.
    val transformed_columns = Seq(
      col("CustomerID"),
      col("TransactionID"),
      col("CustGender").alias("Gender"),
      col("CustAccountBalance").cast("double").alias("Balance"),
      round(col("TransactionAmount (INR)"), 2).alias("TransactionAmountRounded")
    )

    val transformed_select_df = read_csv_df.select(transformed_columns: _*)

    showDataFrameDetails("11 - Select con Seq[Column], alias e funzioni", transformed_select_df.limit(100))

    // Uso della lista anche in altri punti del codice.
    //
    // Una lista di nomi colonna non serve solo per select.
    // Possiamo usarla anche per:
    //
    // - controllare quali colonne stampare;
    // - applicare na.drop su un insieme dinamico;
    // - costruire ordinamenti o aggregazioni.
    //
    // Qui usiamo la lista per eliminare righe che hanno null nelle colonne
    // fondamentali.
    val required_columns = Seq("CustomerID", "TransactionID", "CustAccountBalance")

    val cleaned_df = read_csv_df
      .na.drop(required_columns)
      .select(required_columns.map(col): _*)

    showDataFrameDetails("12 - Lista riusata per na.drop e select", cleaned_df.limit(100))

    printSection("FINE - Job completato")
    println("Lo script ha mostrato select diretto, select con List.map(col): _*, lista dinamica, validazione e Seq[Column].")

    spark.stop()
  }
}
