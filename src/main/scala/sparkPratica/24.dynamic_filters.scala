package sparkPratica

// sbt "runMain sparkPratica.obj_dynamic_filters"
//
// Scopo dello script
// ------------------
// Questo script continua l'argomento dello script 23:
//
// usare liste e configurazioni per guidare il codice Spark.
//
// Nel 23 abbiamo usato liste per:
//
// - select dinamiche;
// - alias;
// - colonne calcolate;
// - ordinamenti;
// - aggregazioni.
//
// Qui facciamo il passo successivo:
//
// costruire filtri dinamici.
//
// Invece di scrivere condizioni fisse nel codice:
//
// df.where(col("Location").isin("MUMBAI", "DELHI"))
//   .where(col("AmountINR") >= 1000)
//
// costruiamo le condizioni da configurazioni:
//
// val selectedLocations = Seq("MUMBAI", "DELHI")
// val minAmount = Some(1000.0)
//
// e poi trasformiamo queste configurazioni in Column Spark.
//
// Idea centrale:
//
// una condizione Spark e' una Column booleana.
//
// Esempi:
//
// col("AmountINR") >= 1000
// col("Location").isin("MUMBAI", "DELHI")
// col("Gender") === "F"
//
// Se abbiamo tante condizioni, possiamo metterle in una Seq[Column] e unirle
// con AND oppure OR.
//
// AND:
//   tutte le condizioni devono essere vere.
//
// OR:
//   basta che almeno una condizione sia vera.
//
// Questo pattern e' utile quando i filtri arrivano da:
//
// - parametri;
// - form di ricerca;
// - file di configurazione;
// - logiche opzionali nel programma.

import org.apache.spark.SparkConf
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object obj_dynamic_filters {
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

  // Unisce una lista di condizioni con AND.
  //
  // Esempio:
  //
  // Seq(
  //   col("AmountINR") >= 1000,
  //   col("Gender") === "F"
  // )
  //
  // diventa:
  //
  // (col("AmountINR") >= 1000) && (col("Gender") === "F")
  //
  // Se la lista e' vuota, restituiamo lit(true).
  //
  // lit(true) significa:
  //
  // "non filtrare nulla".
  //
  // Questo e' comodo quando i filtri sono opzionali: se l'utente non sceglie
  // nessun filtro, il DataFrame resta invariato.
  private def andAll(conditions: Seq[Column]): Column = {
    conditions.reduceOption(_ && _).getOrElse(lit(true))
  }

  // Unisce una lista di condizioni con OR.
  //
  // Esempio:
  //
  // Seq(
  //   col("Location") === "MUMBAI",
  //   col("Location") === "DELHI"
  // )
  //
  // diventa:
  //
  // (col("Location") === "MUMBAI") || (col("Location") === "DELHI")
  //
  // Se la lista e' vuota, restituiamo lit(false), cioe' nessuna riga passa.
  // Per un gruppo OR questo di solito e' il comportamento piu prudente.
  private def orAll(conditions: Seq[Column]): Column = {
    conditions.reduceOption(_ || _).getOrElse(lit(false))
  }

  // Restituisce solo i nomi colonna presenti nel DataFrame.
  //
  // Lo stesso principio visto nello script 23 per le select dinamiche vale
  // anche per i filtri: prima di costruire col("NomeColonna"), conviene sapere
  // se la colonna esiste.
  private def existingColumns(df: DataFrame, requestedColumns: Seq[String]): Seq[String] = {
    val availableColumns = df.columns.toSet
    requestedColumns.filter(availableColumns.contains)
  }

  private def missingColumns(df: DataFrame, requestedColumns: Seq[String]): Seq[String] = {
    val availableColumns = df.columns.toSet
    requestedColumns.filterNot(availableColumns.contains)
  }

  // Configurazione dei filtri del report.
  //
  // Usiamo una case class per raccogliere in un solo oggetto le scelte del
  // report:
  //
  // locations
  //   localita' ammesse.
  //
  // genders
  //   generi ammessi.
  //
  // minAmount / maxAmount
  //   range opzionale dell'importo transazione.
  //
  // minBalance
  //   saldo minimo opzionale.
  //
  // Option[Double] permette di distinguere:
  //
  // Some(1000.0)
  //   filtro presente.
  //
  // None
  //   filtro non richiesto.
  private case class ReportFilters(
    locations: Seq[String],
    genders: Seq[String],
    minAmount: Option[Double],
    maxAmount: Option[Double],
    minBalance: Option[Double]
  )

  // Costruisce la lista di Column booleane partendo dalla configurazione.
  //
  // Nota:
  // non applichiamo subito .where(...).
  // Prima costruiamo una Seq[Column], poi decidiamo come combinarla.
  //
  // Questo separa:
  //
  // - la configurazione dei filtri;
  // - la costruzione delle condizioni;
  // - l'applicazione al DataFrame.
  private def buildFilterConditions(filters: ReportFilters): Seq[Column] = {
    val locationCondition =
      if (filters.locations.nonEmpty) {
        Some(col("Location").isin(filters.locations: _*))
      } else {
        None
      }

    val genderCondition =
      if (filters.genders.nonEmpty) {
        Some(col("Gender").isin(filters.genders: _*))
      } else {
        None
      }

    val minAmountCondition = filters.minAmount.map(value => col("AmountINR") >= value)
    val maxAmountCondition = filters.maxAmount.map(value => col("AmountINR") <= value)
    val minBalanceCondition = filters.minBalance.map(value => col("Balance") >= value)

    Seq(
      locationCondition,
      genderCondition,
      minAmountCondition,
      maxAmountCondition,
      minBalanceCondition
    ).flatten
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("dynamic-filters")
      .setMaster("local[*]")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    printSection("AVVIO - Filtri dinamici")
    println("Leggo bank_transactions.csv e mostro come costruire where dinamici da liste e configurazioni.")

    // Lettura del CSV originale.
    //
    // Continuiamo a usare bank_transactions.csv, come negli script 21, 22 e 23.
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
    val read_csv_df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(BankTransactionsPath)
      .persist()

    showDataFrameDetails("1 - CSV originale limitato alle prime righe", read_csv_df.limit(10))

    // Normalizzazione iniziale.
    //
    // Per rendere gli esempi piu leggibili, rinominiamo alcune colonne:
    //
    // CustGender                -> Gender
    // CustLocation              -> Location
    // CustAccountBalance        -> Balance
    // TransactionAmount (INR)   -> AmountINR
    //
    // Inoltre:
    //
    // - Location viene pulita con trim e upper;
    // - Balance e AmountINR vengono castati a double;
    // - eliminiamo righe senza valori fondamentali.
    val transactions_df = read_csv_df
      .select(
        col("CustomerID").alias("Customer"),
        col("TransactionID").alias("Transaction"),
        col("CustGender").alias("Gender"),
        upper(trim(col("CustLocation"))).alias("Location"),
        col("CustAccountBalance").cast("double").alias("Balance"),
        col("TransactionAmount (INR)").cast("double").alias("AmountINR")
      )
      .na.drop(Seq("Customer", "Transaction", "Location", "Balance", "AmountINR"))
      .persist()

    showDataFrameDetails("2 - Dataset normalizzato per i filtri", transactions_df.limit(100))

    // Filtro scritto in modo statico.
    //
    // Questo e' il modo piu diretto quando le condizioni sono poche e fisse:
    //
    // Location in MUMBAI / DELHI
    // AmountINR >= 1000
    //
    // Funziona bene, ma non e' flessibile.
    // Se domani cambia la lista delle localita' o il valore minimo, dobbiamo
    // modificare il codice.
    val static_filtered_df = transactions_df
      .where(col("Location").isin("MUMBAI", "DELHI"))
      .where(col("AmountINR") >= 1000.0)
      .orderBy(col("AmountINR").desc)

    showDataFrameDetails("3 - Filtro statico scritto direttamente nel codice", static_filtered_df.limit(100))

    // Stessa logica, ma con configurazioni.
    //
    // selectedLocations e selectedGenders sono liste.
    // minAmount, maxAmount e minBalance sono Option.
    //
    // Questo permette di simulare filtri che arrivano dall'esterno.
    //
    // Esempio:
    //
    // se non vogliamo filtrare per maxAmount:
    //
    // maxAmount = None
    //
    // se vogliamo filtrare per maxAmount:
    //
    // maxAmount = Some(50000.0)
    val filters = ReportFilters(
      locations = Seq("MUMBAI", "DELHI", "NEW DELHI", "BANGALORE"),
      genders = Seq("F", "M"),
      minAmount = Some(1000.0),
      maxAmount = Some(50000.0),
      minBalance = Some(10000.0)
    )

    printSection("4 - Configurazione dei filtri")
    println(s"Localita': ${filters.locations.mkString(", ")}")
    println(s"Gender: ${filters.genders.mkString(", ")}")
    println(s"Importo minimo: ${filters.minAmount.getOrElse("non impostato")}")
    println(s"Importo massimo: ${filters.maxAmount.getOrElse("non impostato")}")
    println(s"Saldo minimo: ${filters.minBalance.getOrElse("non impostato")}")

    // Validazione delle colonne usate dai filtri.
    //
    // Nello script 23 abbiamo validato le colonne prima della select.
    // Qui facciamo la stessa cosa prima di costruire filtri dinamici.
    //
    // Se una configurazione chiedesse di filtrare su una colonna non presente,
    // potremmo intercettarlo subito e stampare un messaggio chiaro.
    val filterColumns = Seq("Location", "Gender", "AmountINR", "Balance", "ColonnaNonPresente")
    val validFilterColumns = existingColumns(transactions_df, filterColumns)
    val missingFilterColumns = missingColumns(transactions_df, filterColumns)

    printSection("5 - Validazione colonne usate dai filtri")
    println(s"Colonne richieste dai filtri: ${filterColumns.mkString(", ")}")
    println(s"Colonne presenti: ${validFilterColumns.mkString(", ")}")
    println(s"Colonne mancanti: ${missingFilterColumns.mkString(", ")}")

    // Costruzione delle condizioni.
    //
    // buildFilterConditions restituisce una Seq[Column].
    //
    // Ogni elemento e' una condizione booleana:
    //
    // Location IN (...)
    // Gender IN (...)
    // AmountINR >= ...
    // AmountINR <= ...
    // Balance >= ...
    //
    // andAll le unisce in una sola condizione con AND.
    val dynamicConditions = buildFilterConditions(filters)
    val dynamicWhereCondition = andAll(dynamicConditions)

    printSection("6 - Condizioni dinamiche costruite")
    println(s"Numero condizioni create: ${dynamicConditions.length}")
    println("Le condizioni vengono combinate con AND tramite andAll(...).")

    val dynamic_filtered_df = transactions_df
      .where(dynamicWhereCondition)
      .orderBy(col("AmountINR").desc, col("Balance").desc)
      .persist()

    showDataFrameDetails("7 - DataFrame filtrato con configurazione dinamica", dynamic_filtered_df.limit(100))

    // Filtri opzionali.
    //
    // Qui mostriamo un caso in cui alcune parti della configurazione sono vuote.
    //
    // locations = Seq.empty
    //   nessun filtro sulla localita'.
    //
    // genders = Seq("F")
    //   teniamo solo Gender = F.
    //
    // minAmount = None
    //   nessun minimo sull'importo.
    //
    // maxAmount = Some(500.0)
    //   teniamo importi fino a 500.
    //
    // buildFilterConditions crea solo le condizioni effettivamente richieste.
    val optionalFilters = ReportFilters(
      locations = Seq.empty,
      genders = Seq("F"),
      minAmount = None,
      maxAmount = Some(500.0),
      minBalance = None
    )

    val optional_filtered_df = transactions_df
      .where(andAll(buildFilterConditions(optionalFilters)))
      .orderBy(col("AmountINR").desc)

    showDataFrameDetails("8 - Filtri opzionali: solo Gender F e AmountINR <= 500", optional_filtered_df.limit(100))

    // Gruppo di condizioni OR.
    //
    // Finora abbiamo usato AND:
    //
    // Location ammessa
    // E Gender ammesso
    // E AmountINR nel range
    //
    // A volte serve OR.
    //
    // Esempio:
    //
    // vogliamo righe che appartengono a una localita' importante
    // OPPURE transazioni molto alte.
    //
    // La lista priorityConditions viene combinata con orAll.
    val priorityLocations = Seq("MUMBAI", "NEW DELHI")
    val priorityConditions = Seq(
      col("Location").isin(priorityLocations: _*),
      col("AmountINR") >= 25000.0
    )

    val priority_df = transactions_df
      .where(orAll(priorityConditions))
      .select(
        col("Customer"),
        col("Transaction"),
        col("Gender"),
        col("Location"),
        round(col("Balance"), 2).alias("Balance"),
        round(col("AmountINR"), 2).alias("AmountINR"),
        when(col("AmountINR") >= 25000.0, lit("HIGH_AMOUNT"))
          .otherwise(lit("PRIORITY_LOCATION"))
          .alias("PriorityReason")
      )
      .orderBy(col("AmountINR").desc)

    showDataFrameDetails("9 - Filtro OR: localita' prioritarie oppure importi alti", priority_df.limit(100))

    // Report aggregato dopo il filtro dinamico.
    //
    // Il DataFrame dynamic_filtered_df contiene solo le righe che rispettano
    // la configurazione principale.
    //
    // Ora possiamo riusare lo stesso risultato per un riepilogo:
    //
    // - numero transazioni;
    // - totale importi;
    // - importo medio;
    // - saldo medio.
    val summary_df = dynamic_filtered_df
      .groupBy(col("Location"), col("Gender"))
      .agg(
        count(lit(1)).alias("Transactions"),
        round(sum(col("AmountINR")), 2).alias("TotalAmount"),
        round(avg(col("AmountINR")), 2).alias("AvgAmount"),
        round(avg(col("Balance")), 2).alias("AvgBalance")
      )
      .orderBy(col("TotalAmount").desc)

    showDataFrameDetails("10 - Riepilogo per Location e Gender dopo filtri dinamici", summary_df)

    // Report finale compatto.
    //
    // Qui prepariamo una vista operativa:
    //
    // - solo poche colonne;
    // - importi arrotondati;
    // - fascia importo calcolata;
    // - ordinamento per importo decrescente.
    val final_report_df = dynamic_filtered_df
      .select(
        col("Customer"),
        col("Transaction"),
        col("Gender"),
        col("Location"),
        round(col("Balance"), 2).alias("Balance"),
        round(col("AmountINR"), 2).alias("AmountINR"),
        when(col("AmountINR") >= 10000.0, lit("HIGH"))
          .when(col("AmountINR") >= 5000.0, lit("MEDIUM"))
          .otherwise(lit("LOW"))
          .alias("AmountBand")
      )
      .orderBy(col("AmountINR").desc, col("Balance").desc)

    showDataFrameDetails("11 - Report finale filtrato e ordinato", final_report_df.limit(100))

    printSection("FINE - Job completato")
    println("Lo script ha mostrato filtri statici, configurazioni, Option, AND, OR, validazione e report aggregati.")

    spark.stop()
  }
}
