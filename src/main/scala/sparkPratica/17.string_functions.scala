package sparkPratica

// sbt "runMain sparkPratica.obj_string_functions"
//
// Scopo dello script
// ------------------
// Questo script mostra le principali funzioni stringa dei DataFrame Spark.
//
// Il video di riferimento introduce le "String Functions" e lavora sul file
// bank_transactions.csv. Le funzioni mostrate sono:
//
// - concat
// - instr
// - length
// - lower
// - upper
// - lpad
// - rpad
// - repeat
// - ltrim
// - rtrim
// - split
// - substring
// - regexp_replace
//
// In questo script riprendiamo quelle funzioni e aggiungiamo commenti prima/dopo
// per chiarire cosa cambia nelle colonne.

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object obj_string_functions {
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
      .setAppName("string-functions")
      .setMaster("local[*]")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    printSection("AVVIO - String Functions")
    println("Leggo bank_transactions.csv e applico le funzioni stringa viste nel video.")

    // Lettura del CSV originale.
    //
    // Prima, file fisico:
    //
    // TransactionID,CustomerID,CustomerDOB,CustGender,CustLocation,CustAccountBalance,TransactionDate,TransactionTime,TransactionAmount (INR)
    // T1,C5841053,10/1/94,F,JAMSHEDPUR,17819.05,2/8/16,143207,25
    //
    // Dopo read.csv:
    //
    // TransactionID | CustomerID | CustomerDOB | CustGender | CustLocation | CustAccountBalance | TransactionDate | TransactionAmount (INR)
    // T1            | C5841053   | 10/1/94     | F          | JAMSHEDPUR   | 17819.05           | 2/8/16          | 25.0
    val read_csv_df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(BankTransactionsPath)
      .persist()

    showDataFrameDetails("1 - CSV originale limitato alle prime righe", read_csv_df.limit(10))

    // Selezioniamo poche colonne per rendere l'output leggibile.
    //
    // Le funzioni stringa lavorano bene su colonne come:
    // - TransactionID: valori come T1, T2, T3;
    // - CustomerID: valori come C5841053;
    // - CustomerDOB: valori come 10/1/94;
    // - CustGender: valori come F o M;
    // - CustLocation: valori come JAMSHEDPUR, JHAJJAR, MUMBAI.
    val selected_df = read_csv_df
      .select(
        col("TransactionID"),
        col("CustomerID"),
        col("CustomerDOB"),
        col("CustGender"),
        col("CustLocation"),
        col("CustAccountBalance"),
        col("TransactionDate"),
        col("TransactionAmount (INR)").alias("Transaction_Amount")
      )
      .limit(1000)
      .persist()

    showDataFrameDetails("2 - Colonne usate per gli esempi stringa", selected_df)

    // concat.
    //
    // concat(strs: Column*) concatena due o piu colonne stringa.
    // Nel video viene creato un id composto usando TransactionID e CustomerID.
    //
    // Prima:
    //
    // TransactionID | CustomerID
    // T1            | C5841053
    //
    // Dopo:
    //
    // Unique_Id
    // T1C5841053
    //
    // Qui aggiungiamo anche un separatore con lit("_") per rendere l'id piu
    // leggibile: T1_C5841053.
    val concat_df = selected_df
      .withColumn("Unique_Id", concat(col("TransactionID"), lit("_"), col("CustomerID")))

    showDataFrameDetails("3 - concat: crea Unique_Id", concat_df)

    // instr e length.
    //
    // instr(str, substring) restituisce la posizione della prima occorrenza
    // della sottostringa. Se la sottostringa non esiste, restituisce 0.
    //
    // length(col) restituisce il numero di caratteri della stringa.
    //
    // Esempio:
    //
    // CustLocation | Loc_Position | Cust_Loc_Length
    // JAMSHEDPUR   | 2            | 10
    //
    // Loc_Position cerca la lettera "A" dentro CustLocation:
    // in JAMSHEDPUR la A e' in seconda posizione.
    val position_length_df = selected_df
      .withColumn("Loc_Position", instr(col("CustLocation"), "A"))
      .withColumn("Cust_Loc_Length", length(col("CustLocation")))

    showDataFrameDetails("4 - instr e length su CustLocation", position_length_df)

    // lower e upper.
    //
    // lower converte una stringa in minuscolo.
    // upper converte una stringa in maiuscolo.
    //
    // Prima:
    //
    // CustLocation
    // JAMSHEDPUR
    //
    // Dopo:
    //
    // lower_cust_loc | upper_cust_loc
    // jamshedpur     | JAMSHEDPUR
    val case_df = selected_df
      .withColumn("lower_cust_loc", lower(col("CustLocation")))
      .withColumn("upper_cust_loc", upper(col("CustLocation")))

    showDataFrameDetails("5 - lower e upper", case_df)

    // lpad e rpad.
    //
    // lpad(str, len, pad) riempie a sinistra fino alla lunghezza indicata.
    // rpad(str, len, pad) riempie a destra fino alla lunghezza indicata.
    //
    // Prima:
    //
    // TransactionID
    // T1
    //
    // Dopo:
    //
    // Trans_Lpad | Trans_Rpad
    // 00000000T1 | T100000000
    //
    // Usiamo lunghezza 10 e carattere "0", come esempio tipico per codici.
    val pad_df = selected_df
      .withColumn("Trans_Lpad", lpad(col("TransactionID"), 10, "0"))
      .withColumn("Trans_Rpad", rpad(col("TransactionID"), 10, "0"))

    showDataFrameDetails("6 - lpad e rpad su TransactionID", pad_df)

    // repeat.
    //
    // repeat(col, n) ripete una stringa n volte.
    //
    // Prima:
    //
    // CustGender
    // F
    //
    // Dopo repeat(CustGender, 2):
    //
    // Repeat_Gender
    // FF
    val repeat_df = selected_df
      .withColumn("Repeat_Gender", repeat(col("CustGender"), 2))

    showDataFrameDetails("7 - repeat su CustGender", repeat_df)

    // ltrim e rtrim.
    //
    // ltrim elimina gli spazi a sinistra.
    // rtrim elimina gli spazi a destra.
    //
    // Il dataset non ha sempre spazi visibili nelle prime righe, quindi creiamo
    // una colonna dimostrativa con spazi artificiali attorno a CustLocation.
    //
    // Prima:
    //
    // Location_With_Spaces
    // "   JAMSHEDPUR   "
    //
    // Dopo:
    //
    // Ltrim_Location | Rtrim_Location
    // "JAMSHEDPUR   " | "   JAMSHEDPUR"
    val trim_df = selected_df
      .withColumn("Location_With_Spaces", concat(lit("   "), col("CustLocation"), lit("   ")))
      .withColumn("Ltrim_Location", ltrim(col("Location_With_Spaces")))
      .withColumn("Rtrim_Location", rtrim(col("Location_With_Spaces")))
      .select(
        col("TransactionID"),
        col("CustLocation"),
        col("Location_With_Spaces"),
        col("Ltrim_Location"),
        col("Rtrim_Location")
      )

    showDataFrameDetails("8 - ltrim e rtrim", trim_df)

    // split.
    //
    // split(str, regex) divide una stringa in un array usando un separatore.
    //
    // CustomerDOB contiene date nel formato giorno/mese/anno, per esempio:
    //
    // 10/1/94
    //
    // Con split(CustomerDOB, "/") otteniamo:
    //
    // ["10", "1", "94"]
    //
    // Poi prendiamo i singoli elementi dell'array:
    // - indice 0: giorno;
    // - indice 1: mese;
    // - indice 2: anno.
    val split_df = selected_df
      .withColumn("DOB_Parts", split(col("CustomerDOB"), "/"))
      .withColumn("DOB_Day", col("DOB_Parts")(0))
      .withColumn("DOB_Month", col("DOB_Parts")(1))
      .withColumn("DOB_Year", col("DOB_Parts")(2))
      .select(
        col("CustomerID"),
        col("CustomerDOB"),
        col("DOB_Parts"),
        col("DOB_Day"),
        col("DOB_Month"),
        col("DOB_Year")
      )

    showDataFrameDetails("9 - split su CustomerDOB", split_df)

    // substring.
    //
    // substring(str, pos, len) estrae una parte della stringa.
    // In Spark SQL le posizioni partono da 1.
    //
    // Prima:
    //
    // CustLocation
    // JAMSHEDPUR
    //
    // Dopo substring(CustLocation, 2, 3):
    //
    // Cust_Location_First3
    // AMS
    //
    // Il nome della colonna mantiene "First3" per seguire il video, ma il valore
    // parte dalla seconda posizione per mostrare chiaramente il parametro pos.
    val substring_df = selected_df
      .withColumn("Cust_Location_First3", substring(col("CustLocation"), 2, 3))

    showDataFrameDetails("10 - substring su CustLocation", substring_df)

    // regexp_replace.
    //
    // regexp_replace(col, pattern, replacement) sostituisce tutte le parti della
    // stringa che corrispondono a una regex.
    //
    // Nel video viene mostrata la sostituzione di una parte del nome localita'.
    // Qui sostituiamo "BAI" con "BAY":
    //
    // Prima:
    //
    // CustLocation
    // MUMBAI
    //
    // Dopo:
    //
    // CustLoc_Replace
    // MUMBAY
    val regexp_replace_df = selected_df
      .withColumn("CustLoc_Replace", regexp_replace(col("CustLocation"), "BAI", "BAY"))

    showDataFrameDetails("11 - regexp_replace su CustLocation", regexp_replace_df)

    // Esempio finale: tutte le funzioni principali in una sola trasformazione.
    //
    // Questo blocco e' vicino al flusso mostrato nel video: una catena di
    // withColumn che aggiunge molte colonne derivate al DataFrame originale.
    val all_string_functions_df = selected_df
      .withColumn("Unique_Id", concat(col("TransactionID"), lit("_"), col("CustomerID")))
      .withColumn("Loc_Position", instr(col("CustLocation"), "A"))
      .withColumn("Cust_Loc_Length", length(col("CustLocation")))
      .withColumn("lower_cust_loc", lower(col("CustLocation")))
      .withColumn("upper_cust_loc", upper(col("CustLocation")))
      .withColumn("Repeat_Gender", repeat(col("CustGender"), 2))
      .withColumn("Cust_Location_First3", substring(col("CustLocation"), 2, 3))
      .withColumn("CustLoc_Replace", regexp_replace(col("CustLocation"), "BAI", "BAY"))
      .withColumn("Trans_Lpad", lpad(col("TransactionID"), 10, "0"))
      .withColumn("Trans_Rpad", rpad(col("TransactionID"), 10, "0"))

    showDataFrameDetails("12 - Tutte le funzioni stringa principali", all_string_functions_df)

    printSection("FINE - Job completato")
    println("Lo script ha mostrato concat, instr, length, lower, upper, lpad, rpad, repeat, ltrim, rtrim, split, substring e regexp_replace.")

    spark.stop()
  }
}
