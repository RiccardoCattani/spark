package sparkPratica

// sbt "runMain sparkPratica.obj_handling_nulls"
//
// Scopo dello script
// ------------------
// Questo script mostra come gestire valori null in un DataFrame Spark.
//
// Il video di riferimento usa un file account.txt separato da pipe "|", con
// colonne:
//
// ACCT_ID|Bank_ID|Gender|Country
//
// e mostra esempi come:
// - read_df.na.drop()
// - read_df.na.drop("all")
// - read_df.na.drop(Seq("Gender"))
// - select + cast
// - na.fill("NA")
//
// In questo script riprendiamo quei passaggi e aggiungiamo commenti prima/dopo,
// conteggi e una sezione per contare i null per colonna.

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object obj_handling_nulls {
  private val MaxRowsToShow = 50
  private val AccountPath = "C:\\repository\\spark\\1.input\\account.txt"

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
      .setAppName("handling-nulls")
      .setMaster("local[*]")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    printSection("AVVIO - Gestione valori null")
    println("Leggo account.txt e mostro drop, drop su subset e fill dei null.")

    // Lettura del file account.txt.
    //
    // Prima, file fisico:
    //
    // ACCT_ID|Bank_ID|Gender|Country
    // 1000234|201|M|IND
    // 1000235||F|IND
    // 1000236|202||IND
    // 1000239|206|M|
    // |209|M|IND
    // |||
    //
    // Dopo read.csv con header=true e delimiter="|":
    //
    // ACCT_ID | Bank_ID | Gender | Country
    // 1000234 | 201     | M      | IND
    // 1000235 | null    | F      | IND
    // 1000236 | 202     | null   | IND
    // 1000239 | 206     | M      | null
    // null    | 209     | M      | IND
    // null    | null    | null   | null
    val read_df = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", "|")
      .load(AccountPath)
      .persist()

    showDataFrameDetails("1 - DataFrame originale con null", read_df)

    // Conta quanti null ci sono per ogni colonna.
    //
    // Risultato atteso sul file di esempio:
    //
    // ACCT_ID | Bank_ID | Gender | Country
    // 2       | 2       | 2      | 2
    //
    // Perche':
    // - ACCT_ID manca nella riga "|209|M|IND" e nella riga "|||";
    // - Bank_ID manca nella riga "1000235||F|IND" e nella riga "|||";
    // - Gender manca nella riga "1000236|202||IND" e nella riga "|||";
    // - Country manca nella riga "1000239|206|M|" e nella riga "|||".
    printSection("2 - Conteggio null per colonna")
    val nullCounts = read_df.columns.map { columnName =>
      sum(when(col(columnName).isNull || trim(col(columnName)) === "", 1).otherwise(0)).alias(columnName)
    }
    read_df.select(nullCounts: _*).show(truncate = false)

    // na.drop() senza parametri.
    //
    // Comportamento:
    // elimina ogni riga che contiene almeno un null in qualunque colonna.
    //
    // Prima:
    //
    // ACCT_ID | Bank_ID | Gender | Country
    // 1000234 | 201     | M      | IND
    // 1000235 | null    | F      | IND
    // 1000236 | 202     | null   | IND
    //
    // Dopo na.drop():
    //
    // ACCT_ID | Bank_ID | Gender | Country
    // 1000234 | 201     | M      | IND
    //
    // Quindi e' una pulizia molto restrittiva: basta un solo null per rimuovere
    // tutta la riga.
    val drop_any_null_df = read_df.na.drop()
    showDataFrameDetails("3 - na.drop(): elimina righe con almeno un null", drop_any_null_df)

    // na.drop("all").
    //
    // Comportamento:
    // elimina solo le righe dove TUTTE le colonne sono null.
    //
    // Prima:
    //
    // ACCT_ID | Bank_ID | Gender | Country
    // null    | 209     | M      | IND
    // null    | null    | null   | null
    //
    // Dopo na.drop("all"):
    //
    // ACCT_ID | Bank_ID | Gender | Country
    // null    | 209     | M      | IND
    //
    // La riga completamente vuota viene rimossa, ma le righe con solo alcuni
    // valori null restano.
    val drop_all_null_df = read_df.na.drop("all")
    showDataFrameDetails("4 - na.drop(\"all\"): elimina solo righe tutte null", drop_all_null_df)

    // na.drop(Seq("Gender")).
    //
    // Comportamento:
    // controlla solo la colonna Gender. Se Gender e' null, la riga viene eliminata.
    // I null nelle altre colonne non contano.
    //
    // Prima:
    //
    // ACCT_ID | Bank_ID | Gender | Country
    // 1000235 | null    | F      | IND
    // 1000236 | 202     | null   | IND
    // 1000239 | 206     | M      | null
    //
    // Dopo na.drop(Seq("Gender")):
    //
    // ACCT_ID | Bank_ID | Gender | Country
    // 1000235 | null    | F      | IND
    // 1000239 | 206     | M      | null
    //
    // La riga con Gender=null viene rimossa. Le altre restano anche se hanno
    // Bank_ID o Country null.
    val drop_null_gender_df = read_df.na.drop(Seq("Gender"))
    showDataFrameDetails("5 - na.drop su subset: controlla solo Gender", drop_null_gender_df)

    // na.drop(Seq("ACCT_ID", "Bank_ID")).
    //
    // Questo e' un altro esempio utile: Spark elimina le righe dove ACCT_ID
    // oppure Bank_ID sono null, ma ignora i null in Gender e Country.
    //
    // Prima:
    //
    // ACCT_ID | Bank_ID | Gender | Country
    // 1000236 | 202     | null   | IND
    // null    | 209     | M      | IND
    // 1000239 | 206     | M      | null
    //
    // Dopo:
    //
    // ACCT_ID | Bank_ID | Gender | Country
    // 1000236 | 202     | null   | IND
    // 1000239 | 206     | M      | null
    val drop_null_account_or_bank_df = read_df.na.drop(Seq("ACCT_ID", "Bank_ID"))
    showDataFrameDetails("6 - na.drop su subset: controlla ACCT_ID e Bank_ID", drop_null_account_or_bank_df)

    // Selezione e cast.
    //
    // Nel video viene mostrato:
    //
    // val select_df = read_df.select(col("ACCT_ID").cast("Int"), col("Gender"))
    //
    // cast("Int") converte ACCT_ID da stringa a intero.
    //
    // Prima:
    //
    // ACCT_ID | Gender
    // 1000234 | M
    // null    | M
    //
    // Dopo:
    //
    // ACCT_ID | Gender
    // 1000234 | M
    // null    | M
    //
    // A video il valore sembra uguale, ma lo schema cambia:
    // ACCT_ID passa da string a integer.
    val select_df = read_df.select(
      col("ACCT_ID").cast("int").alias("ACCT_ID"),
      col("Gender")
    )
    showDataFrameDetails("7 - Select ACCT_ID cast int e Gender", select_df)

    // na.fill("NA") su DataFrame con colonne stringa.
    //
    // fill sostituisce i null con un valore.
    // Qui usiamo "NA", quindi Spark puo' riempire le colonne stringa.
    //
    // Prima:
    //
    // ACCT_ID | Gender
    // 1000236 | null
    // null    | M
    //
    // Dopo select_df.na.fill("NA"):
    //
    // ACCT_ID | Gender
    // 1000236 | NA
    // null    | M
    //
    // Attenzione: ACCT_ID e' stato convertito in int, quindi fill("NA") non puo'
    // riempire ACCT_ID con una stringa. Riempie invece Gender.
    val fill_string_df = select_df.na.fill("NA")
    showDataFrameDetails("8 - na.fill(\"NA\") dopo cast: riempie Gender ma non ACCT_ID intero", fill_string_df)

    // Fill con valori diversi per colonne diverse.
    //
    // Questo approccio e' piu controllato:
    // - ACCT_ID null diventa -1;
    // - Gender null diventa "NA".
    //
    // Prima:
    //
    // ACCT_ID | Gender
    // null    | M
    // 1000236 | null
    //
    // Dopo:
    //
    // ACCT_ID | Gender
    // -1      | M
    // 1000236 | NA
    val fill_map_df = select_df.na.fill(Map(
      "ACCT_ID" -> -1,
      "Gender" -> "NA"
    ))
    showDataFrameDetails("9 - na.fill con Map: valori diversi per colonne diverse", fill_map_df)

    printSection("FINE - Job completato")
    println("Lo script ha mostrato na.drop, na.drop(\"all\"), na.drop su subset, cast e na.fill.")

    spark.stop()
  }
}
