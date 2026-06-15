package sparkPratica

// sbt "runMain sparkPratica.obj_working_with_dates"
//
// Scopo dello script
// ------------------
// Questo script mostra le principali funzioni data dei DataFrame Spark.
//
// Il video di riferimento usa il file bank_transactions.csv e introduce le
// funzioni "Working with Dates", tra cui:
//
// - current_date
// - date_format
// - to_date
// - add_months
// - date_add
// - datediff
// - months_between
// - next_day
// - trunc
// - year
// - quarter
// - month
// - dayofweek
// - dayofmonth
// - dayofyear
// - weekofyear
// - last_day
//
// In questo script riprendiamo quei passaggi e aggiungiamo commenti didattici
// prima/dopo per chiarire cosa produce ogni trasformazione.

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object obj_working_with_dates {
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
      .setAppName("working-with-dates")
      .setMaster("local[*]")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    printSection("AVVIO - Working with Dates")
    println("Leggo bank_transactions.csv e applico le funzioni data viste nel video.")

    // Lettura del CSV originale.
    //
    // Prima, file fisico:
    //
    // TransactionID,CustomerID,CustomerDOB,CustGender,CustLocation,CustAccountBalance,TransactionDate,TransactionTime,TransactionAmount (INR)
    // T1,C5841053,10/1/94,F,JAMSHEDPUR,17819.05,2/8/16,143207,25
    //
    // Dopo read.csv:
    //
    // TransactionID | CustomerID | CustomerDOB | TransactionDate
    // T1            | C5841053   | 10/1/94     | 2/8/16
    val read_csv_df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(BankTransactionsPath)
      .persist()

    showDataFrameDetails("1 - CSV originale limitato alle prime righe", read_csv_df.limit(10))

    // Selezioniamo le colonne utili per gli esempi sulle date.
    //
    // CustomerDOB e TransactionDate nel CSV sono stringhe, non date Spark.
    // Per lavorare davvero con funzioni data dobbiamo convertirle con to_date.
    val selected_df = read_csv_df
      .select(
        col("TransactionID"),
        col("CustomerID"),
        col("CustomerDOB"),
        col("TransactionDate"),
        col("CustGender"),
        col("CustLocation"),
        col("TransactionAmount (INR)").alias("Transaction_Amount")
      )
      .limit(1000)
      .persist()

    showDataFrameDetails("2 - Colonne usate per gli esempi data", selected_df)

    // current_date.
    //
    // current_date() restituisce la data corrente del sistema come tipo date.
    //
    // Prima:
    //
    // CustomerDOB
    // 10/1/94
    //
    // Dopo:
    //
    // CustomerDOB | Current_Date
    // 10/1/94     | data di oggi
    //
    // Nota: il valore cambia in base al giorno in cui esegui lo script.
    val current_date_df = selected_df
      .select(col("CustomerDOB"))
      .withColumn("Current_Date", current_date())

    showDataFrameDetails("3 - current_date: aggiunge la data corrente", current_date_df)

    // date_format.
    //
    // date_format(dateExpr, format) converte una data in stringa usando il
    // formato indicato.
    //
    // Esempio:
    //
    // Current_Date | Current_Date_Formatted
    // 2026-06-15   | 06-15-2026
    //
    // Qui usiamo MM-dd-yyyy:
    // - MM: mese a due cifre;
    // - dd: giorno a due cifre;
    // - yyyy: anno a quattro cifre.
    val date_format_df = current_date_df
      .withColumn("Current_Date_Formatted", date_format(col("Current_Date"), "MM-dd-yyyy"))

    showDataFrameDetails("4 - date_format sulla data corrente", date_format_df)

    // Preparazione di CustomerDOB per to_date.
    //
    // Nel file CustomerDOB ha valori misti:
    //
    // 10/1/94
    // 1/1/1800
    //
    // Il primo usa anno a due cifre, il secondo anno a quattro cifre.
    // Per evitare ambiguita', trasformiamo gli anni a due cifre in anni a
    // quattro cifre aggiungendo "19" davanti.
    //
    // Prima:
    //
    // CustomerDOB
    // 10/1/94
    //
    // Dopo:
    //
    // CustomerDOB_Normalized
    // 10/1/1994
    val normalized_dates_df = selected_df
      .withColumn("DOB_Parts", split(col("CustomerDOB"), "/"))
      .withColumn(
        "CustomerDOB_Normalized",
        when(
          size(col("DOB_Parts")) === 3 && length(col("DOB_Parts")(2)) === 2,
          concat(col("DOB_Parts")(0), lit("/"), col("DOB_Parts")(1), lit("/19"), col("DOB_Parts")(2))
        ).otherwise(col("CustomerDOB"))
      )
      .drop("DOB_Parts")

    showDataFrameDetails("5 - Normalizzazione CustomerDOB prima di to_date", normalized_dates_df)

    // to_date.
    //
    // to_date(col, format) converte una stringa in una vera data Spark.
    //
    // Prima:
    //
    // CustomerDOB_Normalized
    // 10/1/1994
    //
    // Dopo:
    //
    // CustomerDOB_Date
    // 1994-01-10
    //
    // Da questo momento Spark puo' usare la colonna per calcoli temporali.
    val to_date_df = normalized_dates_df
      .withColumn("CustomerDOB_Date", to_date(col("CustomerDOB_Normalized"), "d/M/yyyy"))
      .withColumn("Transaction_Date", to_date(col("TransactionDate"), "d/M/yy"))

    showDataFrameDetails("6 - to_date: converte stringhe in date", to_date_df)

    // add_months e date_add.
    //
    // add_months(startDate, numMonths) aggiunge mesi a una data.
    // date_add(startDate, days) aggiunge giorni a una data.
    //
    // Prima:
    //
    // Current_Date
    // 2026-06-15
    //
    // Dopo:
    //
    // Date_Plus_Month | Date_Plus_Days
    // 2026-07-15      | 2026-06-22
    val add_dates_df = to_date_df
      .withColumn("Current_Date", current_date())
      .withColumn("Current_Date_Formatted", date_format(col("Current_Date"), "MM-dd-yyyy"))
      .withColumn("Date_Plus_Month", add_months(col("Current_Date"), 1))
      .withColumn("Date_Plus_Days", date_add(col("Current_Date"), 7))

    showDataFrameDetails("7 - add_months e date_add", add_dates_df)

    // datediff e months_between.
    //
    // datediff(end, start) restituisce il numero di giorni tra due date.
    // months_between(end, start) restituisce il numero di mesi tra due date.
    //
    // Qui calcoliamo:
    // - giorni trascorsi dalla data di nascita a oggi;
    // - mesi trascorsi dalla data di nascita a oggi.
    //
    // Il risultato e' grande perche' CustomerDOB e' una data di nascita.
    val diff_dates_df = add_dates_df
      .withColumn("DOB_Plus_Days", datediff(col("Current_Date"), col("CustomerDOB_Date")))
      .withColumn("DOB_Plus_Months", months_between(col("Current_Date"), col("CustomerDOB_Date")))

    showDataFrameDetails("8 - datediff e months_between", diff_dates_df)

    // next_day e trunc.
    //
    // next_day(date, dayOfWeek) restituisce la prima data successiva che cade
    // nel giorno della settimana indicato.
    //
    // trunc(date, format) tronca la data al livello richiesto.
    //
    // Esempi:
    //
    // next_day(Current_Date, "Monday") cerca il prossimo lunedi.
    // trunc(Current_Date, "year") porta la data al primo giorno dell'anno.
    val next_trunc_df = diff_dates_df
      .withColumn("Next_Monday", next_day(col("Current_Date"), "Monday"))
      .withColumn("Start_Of_Year", trunc(col("Current_Date"), "year"))
      .withColumn("Start_Of_Month", trunc(col("Current_Date"), "month"))

    showDataFrameDetails("9 - next_day e trunc", next_trunc_df)

    // year, quarter e month.
    //
    // Queste funzioni estraggono parti della data:
    // - year: anno;
    // - quarter: trimestre;
    // - month: mese.
    //
    // Prima:
    //
    // Current_Date
    // 2026-06-15
    //
    // Dopo:
    //
    // Current_Year | Current_Quarter | Current_Month
    // 2026         | 2               | 6
    val year_month_df = next_trunc_df
      .withColumn("Current_Year", year(col("Current_Date")))
      .withColumn("Current_Quarter", quarter(col("Current_Date")))
      .withColumn("Current_Month", month(col("Current_Date")))

    showDataFrameDetails("10 - year, quarter e month", year_month_df)

    // dayofweek, dayofmonth, dayofyear, weekofyear e last_day.
    //
    // dayofweek restituisce il giorno della settimana come numero:
    // 1 = domenica, 2 = lunedi, ..., 7 = sabato.
    //
    // dayofmonth restituisce il giorno del mese.
    // dayofyear restituisce il giorno dell'anno.
    // weekofyear restituisce la settimana dell'anno.
    // last_day restituisce l'ultimo giorno del mese.
    val day_parts_df = year_month_df
      .withColumn("Day_Of_Week", dayofweek(col("Current_Date")))
      .withColumn("Day_Of_Month", dayofmonth(col("Current_Date")))
      .withColumn("Day_Of_Year", dayofyear(col("Current_Date")))
      .withColumn("Week_Of_Year", weekofyear(col("Current_Date")))
      .withColumn("Last_Day_Of_Month", last_day(col("Current_Date")))

    showDataFrameDetails("11 - day e week functions", day_parts_df)

    // Esempio finale compatto: colonne principali viste nel video.
    //
    // Manteniamo solo le colonne piu importanti per leggere il risultato finale
    // senza avere una tabella troppo larga.
    val final_dates_df = day_parts_df
      .select(
        col("CustomerDOB"),
        col("CustomerDOB_Date"),
        col("Current_Date"),
        col("Current_Date_Formatted"),
        col("Date_Plus_Month"),
        col("Date_Plus_Days"),
        col("DOB_Plus_Days"),
        col("DOB_Plus_Months"),
        col("Next_Monday"),
        col("Start_Of_Year"),
        col("Current_Year"),
        col("Current_Quarter"),
        col("Current_Month"),
        col("Day_Of_Week"),
        col("Day_Of_Month"),
        col("Day_Of_Year"),
        col("Week_Of_Year"),
        col("Last_Day_Of_Month")
      )

    showDataFrameDetails("12 - Riepilogo funzioni data principali", final_dates_df)

    printSection("FINE - Job completato")
    println("Lo script ha mostrato current_date, date_format, to_date, add_months, date_add, datediff, months_between, next_day, trunc, year, quarter, month, dayofweek, dayofmonth, dayofyear, weekofyear e last_day.")

    spark.stop()
  }
}
