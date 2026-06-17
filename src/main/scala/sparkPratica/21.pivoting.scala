package sparkPratica

// sbt "runMain sparkPratica.obj_pivoting"
//
// Scopo dello script
// ------------------
// Questo script mostra come usare pivot nei DataFrame Spark.
//
// Il video di riferimento usa il file bank_transactions.csv e mostra:
//
// - lettura del CSV
// - selezione di CustGender, CustLocation e CustAccountBalance
// - groupBy("CustLocation").pivot("CustGender").sum("CustAccountBalance")
//
// pivot trasforma i valori distinti di una colonna in nuove colonne.
//
// In pratica passiamo da una tabella "lunga" a una tabella "larga".
//
// Tabella lunga:
// - ogni riga contiene una categoria nella colonna CustGender;
// - il valore F/M/T e' scritto come dato dentro una cella.
//
// Tabella larga:
// - F, M e T diventano nomi di colonne;
// - dentro quelle colonne troviamo una misura aggregata, ad esempio la somma
//   del saldo.
//
// Esempio logico:
//
// CustLocation | CustGender | CustAccountBalance
// MUMBAI       | F          | 100
// MUMBAI       | M          | 200
// DELHI        | F          | 300
//
// Dopo pivot("CustGender"):
//
// CustLocation | F   | M
// MUMBAI       | 100 | 200
// DELHI        | 300 | null
//
// Il null significa: per quella localita' non esiste nessuna riga con quel
// valore di CustGender.
//
// Quindi pivot e' utile quando vogliamo creare una tabella "larga", simile a
// una tabella pivot di Excel: una colonna resta come gruppo, una colonna diventa
// intestazione, e una misura viene aggregata.
//
// Struttura mentale:
//
// groupBy(...)
//   decide quali righe devono stare insieme.
//
// pivot(...)
//   decide quale colonna deve diventare intestazione.
//
// sum/avg/count/agg(...)
//   decide quale valore calcolare dentro ogni incrocio.

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object obj_pivoting {
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
      .setAppName("pivoting")
      .setMaster("local[*]")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    printSection("AVVIO - Pivoting")
    println("Leggo bank_transactions.csv e mostro come creare tabelle pivot con groupBy + pivot + aggregazione.")

    // Lettura del CSV originale.
    //
    // Prima, file fisico:
    //
    // TransactionID,CustomerID,CustomerDOB,CustGender,CustLocation,CustAccountBalance,TransactionDate,TransactionTime,TransactionAmount (INR)
    // T1,C5841053,10/1/94,F,JAMSHEDPUR,17819.05,2/8/16,143207,25
    //
    // Dopo read.csv:
    //
    // CustGender | CustLocation | CustAccountBalance
    // F          | JAMSHEDPUR   | 17819.05
    //
    // Usiamo inferSchema=true per far capire a Spark i tipi delle colonne.
    // Per esempio CustAccountBalance viene letto come numero se il contenuto
    // del CSV lo permette.
    //
    // La persist() serve per mantenere il DataFrame in memoria dopo la prima
    // azione. In questo script leggiamo lo stesso dataset piu volte per diversi
    // esempi, quindi e' utile evitare di ricaricare sempre il CSV da disco.
    val read_csv_df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(BankTransactionsPath)
      .persist()

    showDataFrameDetails("1 - CSV originale limitato alle prime righe", read_csv_df.limit(10))

    // Selezioniamo le colonne usate nel video.
    //
    // CustAccountBalance viene castato a double per permettere somme numeriche.
    // Se Spark lo leggesse come stringa, sum non avrebbe il significato corretto.
    //
    // Teniamo solo:
    //
    // CustGender
    //   diventera' la colonna da trasformare in intestazioni con pivot.
    //
    // CustLocation
    //   sara' la colonna di raggruppamento con groupBy.
    //
    // CustAccountBalance
    //   sara' la misura numerica da sommare.
    //
    // La drop sui null rimuove righe senza localita' o senza saldo.
    // Non rimuoviamo qui i null di CustGender perche' nel primo pivot vogliamo
    // vedere cosa succede quando la colonna pivot contiene valori null.
    val df = read_csv_df
      .select(
        col("CustGender"),
        col("CustLocation"),
        col("CustAccountBalance").cast("double").alias("CustAccountBalance")
      )
      .na.drop(Seq("CustLocation", "CustAccountBalance"))
      .persist()

    showDataFrameDetails("2 - Colonne selezionate per il pivot", df.limit(1000))

    // Controllo dei valori distinti della colonna che useremo per pivot.
    //
    // Ogni valore distinto di CustGender puo' diventare una colonna.
    // Nel dataset possono comparire F, M, T e anche valori null.
    //
    // Questo controllo e' importante prima di fare pivot:
    //
    // - se la colonna ha pochi valori distinti, il pivot e' leggibile;
    // - se la colonna ha migliaia di valori distinti, il pivot crea migliaia di
    //   colonne e puo' diventare pesante;
    // - se ci sono null, Spark puo' creare anche una colonna chiamata null nel
    //   risultato del pivot.
    //
    // count(lit(1)) conta quante righe ci sono per ogni valore di CustGender.
    // Usiamo lit(1) per contare le righe indipendentemente dai null nelle altre
    // colonne.
    val gender_values_df = df
      .groupBy(col("CustGender"))
      .agg(count(lit(1)).alias("NumeroRighe"))
      .orderBy(col("CustGender").asc_nulls_first)

    showDataFrameDetails("3 - Valori distinti di CustGender", gender_values_df)

    // Pivot base, come nel video.
    //
    // groupBy("CustLocation")
    //   mantiene una riga per ogni localita'.
    //   Tutte le transazioni della stessa localita' finiscono nello stesso
    //   gruppo logico.
    //
    // pivot("CustGender")
    //   trasforma i valori della colonna CustGender in colonne.
    //   Se nel dataset esistono F, M, T e null, il risultato puo' avere colonne:
    //
    //   CustLocation | null | F | M | T
    //
    // sum("CustAccountBalance")
    //   decide quale misura aggregare dentro ogni incrocio localita' + genere.
    //   Per esempio:
    //
    //   MUMBAI + F = somma di CustAccountBalance per le righe di MUMBAI con F
    //   MUMBAI + M = somma di CustAccountBalance per le righe di MUMBAI con M
    //
    // Risultato:
    //
    // CustLocation | null | F       | M       | T
    // MUMBAI       | ...  | somma F | somma M | somma T
    //
    // Nota:
    // questo pivot e' "automatico": Spark scopre da solo i valori distinti di
    // CustGender. E' comodo per esplorare, ma meno controllato in produzione.
    val pivot_raw_df = df
      .groupBy("CustLocation")
      .pivot("CustGender")
      .sum("CustAccountBalance")
      .orderBy(col("CustLocation"))

    showDataFrameDetails("4 - Pivot base: saldo totale per localita' e genere", pivot_raw_df.limit(100))

    // Pivot con valori espliciti.
    //
    // Quando scriviamo pivot("CustGender") senza lista valori, Spark deve
    // scoprire i valori distinti della colonna prima di costruire le colonne.
    // Questo richiede un passaggio aggiuntivo sul dataset.
    //
    // Se conosciamo gia' i valori desiderati, e' meglio passarli esplicitamente:
    //
    // pivot("CustGender", Seq("F", "M", "T"))
    //
    // In questo modo:
    // - controlliamo l'ordine delle colonne;
    // - evitiamo la colonna null;
    // - rendiamo l'operazione piu prevedibile.
    //
    // Prima filtriamo:
    //
    // where(col("CustGender").isin("F", "M", "T"))
    //
    // cosi' escludiamo valori non desiderati, inclusi i null.
    //
    // Poi usiamo:
    //
    // .na.fill(0.0, Seq("F", "M", "T"))
    //
    // Dopo il pivot, un null significa "non c'erano righe per quell'incrocio".
    // Per calcoli successivi e' spesso piu comodo sostituire quel null con 0.0.
    //
    // Esempio:
    //
    // CustLocation | F    | M
    // DELHI        | 300  | null
    //
    // diventa:
    //
    // CustLocation | F    | M
    // DELHI        | 300  | 0
    val pivot_gender_df = df
      .where(col("CustGender").isin("F", "M", "T"))
      .groupBy("CustLocation")
      .pivot("CustGender", Seq("F", "M", "T"))
      .sum("CustAccountBalance")
      .na.fill(0.0, Seq("F", "M", "T"))
      .orderBy(col("CustLocation"))
      .persist()

    showDataFrameDetails("5 - Pivot con valori espliciti F, M, T", pivot_gender_df.limit(100))

    // Aggiungiamo colonne calcolate sulla tabella pivot.
    //
    // Dopo il pivot, F, M e T sono colonne normali.
    // Possiamo quindi calcolare:
    // - TotalBalance: somma dei saldi nella localita';
    // - FemaleShare: quota del saldo associata a F.
    //
    // Esempio:
    //
    // CustLocation | F   | M   | T
    // MUMBAI       | 100 | 300 | 0
    //
    // TotalBalance = 100 + 300 + 0 = 400
    // FemaleShare  = 100 / 400 = 0.25
    //
    // Usiamo when(col("TotalBalance") > 0, ...)
    // per evitare divisioni per zero.
    //
    // round(..., 4) arrotonda la quota a 4 decimali.
    val pivot_with_totals_df = pivot_gender_df
      .withColumn("TotalBalance", col("F") + col("M") + col("T"))
      .withColumn(
        "FemaleShare",
        when(col("TotalBalance") > 0, round(col("F") / col("TotalBalance"), 4))
          .otherwise(lit(0.0))
      )
      .orderBy(col("TotalBalance").desc)

    showDataFrameDetails("6 - Pivot con totale e quota F", pivot_with_totals_df.limit(100))

    // Pivot con piu misure aggregate.
    //
    // Finora abbiamo calcolato solo la somma del saldo.
    // Con agg possiamo calcolare piu metriche nello stesso pivot.
    //
    // Qui per ogni combinazione CustLocation + CustGender calcoliamo:
    //
    // round(sum("CustAccountBalance"), 2).alias("BalanceSum")
    //   somma dei saldi arrotondata a 2 decimali.
    //
    // count(lit(1)).alias("Rows")
    //   numero di righe che contribuiscono a quella somma.
    //
    // Per ogni localita' e per ogni genere otteniamo:
    // - somma del saldo;
    // - numero di righe.
    //
    // Il risultato ha colonne generate combinando valore del pivot e nome della
    // metrica:
    //
    // F_BalanceSum | F_Rows | M_BalanceSum | M_Rows | T_BalanceSum | T_Rows
    //
    // Questa forma e' utile quando vogliamo sapere non solo il totale, ma anche
    // quante osservazioni hanno prodotto quel totale.
    val pivot_multi_metric_df = df
      .where(col("CustGender").isin("F", "M", "T"))
      .groupBy("CustLocation")
      .pivot("CustGender", Seq("F", "M", "T"))
      .agg(
        round(sum("CustAccountBalance"), 2).alias("BalanceSum"),
        count(lit(1)).alias("Rows")
      )
      .orderBy(col("CustLocation"))

    showDataFrameDetails("7 - Pivot con piu metriche: sum e count", pivot_multi_metric_df.limit(100))

    // Pivot su un'altra colonna.
    //
    // Non siamo obbligati a usare CustGender come colonna pivot.
    // Qui raggruppiamo per genere e trasformiamo alcune localita' note in
    // colonne, sommando il saldo per ogni incrocio.
    //
    // Rispetto al pivot precedente invertiamo il punto di vista:
    //
    // Prima:
    //   righe = CustLocation
    //   colonne = CustGender
    //
    // Ora:
    //   righe = CustGender
    //   colonne = CustLocation
    //
    // Selezioniamo solo alcune localita' per evitare una tabella troppo larga.
    // Fare pivot su CustLocation senza limitare i valori creerebbe una colonna
    // per ogni localita' distinta, quindi molte colonne.
    val selected_locations = Seq("MUMBAI", "NEW DELHI", "BANGALORE", "GURGAON")

    val pivot_location_df = df
      .where(col("CustGender").isin("F", "M"))
      .where(col("CustLocation").isin(selected_locations: _*))
      .groupBy("CustGender")
      .pivot("CustLocation", selected_locations)
      .agg(round(sum("CustAccountBalance"), 2))
      .na.fill(0.0, selected_locations)
      .orderBy(col("CustGender"))

    showDataFrameDetails("8 - Pivot inverso: localita' come colonne", pivot_location_df)

    // Unpivot: tornare da tabella larga a tabella lunga.
    //
    // Spark non ha solo pivot. Possiamo anche fare l'operazione inversa con
    // stack dentro selectExpr.
    //
    // Unpivot significa tornare dalla forma larga alla forma lunga.
    //
    // Prima:
    //
    // CustLocation | F   | M   | T
    // MUMBAI       | 100 | 300 | 0
    //
    // Dopo:
    //
    // CustLocation | CustGender | BalanceByGender
    // MUMBAI       | F          | 100
    // MUMBAI       | M          | 300
    // MUMBAI       | T          | 0
    //
    // La funzione stack funziona cosi':
    //
    // stack(3, 'F', F, 'M', M, 'T', T)
    //
    // 3
    //   indica quante righe vogliamo generare per ogni riga originale.
    //
    // 'F', F
    //   crea una riga con etichetta F e valore preso dalla colonna F.
    //
    // 'M', M
    //   crea una riga con etichetta M e valore preso dalla colonna M.
    //
    // 'T', T
    //   crea una riga con etichetta T e valore preso dalla colonna T.
    //
    // Questo formato lungo e' spesso piu comodo per ulteriori groupBy, filtri
    // o salvataggi in formato normalizzato.
    //
    // Il filtro BalanceByGender > 0 rimuove gli incroci senza valore reale,
    // cioe' quelli che avevamo riempito con 0 dopo il pivot.
    val unpivot_df = pivot_gender_df
      .selectExpr(
        "CustLocation",
        "stack(3, 'F', F, 'M', M, 'T', T) as (CustGender, BalanceByGender)"
      )
      .where(col("BalanceByGender") > 0)
      .orderBy(col("CustLocation"), col("CustGender"))

    showDataFrameDetails("9 - Unpivot con stack", unpivot_df.limit(100))

    // Esempio finale compatto.
    //
    // Teniamo le localita' con saldo totale maggiore, per ottenere una tabella
    // pivot piu leggibile.
    //
    // Qui prepariamo una vista finale piu pulita:
    //
    // - rinominiamo F, M e T in F_Balance, M_Balance e T_Balance;
    // - arrotondiamo i valori a 2 decimali;
    // - ordiniamo per TotalBalance decrescente;
    // - mostriamo solo le prime 50 righe con limit(50).
    //
    // Questa e' la forma piu simile a un report finale: una riga per localita',
    // colonne separate per genere, totale e quota femminile.
    val final_pivot_df = pivot_with_totals_df
      .select(
        col("CustLocation"),
        round(col("F"), 2).alias("F_Balance"),
        round(col("M"), 2).alias("M_Balance"),
        round(col("T"), 2).alias("T_Balance"),
        round(col("TotalBalance"), 2).alias("TotalBalance"),
        col("FemaleShare")
      )
      .orderBy(col("TotalBalance").desc)

    showDataFrameDetails("10 - Riepilogo finale: top localita' per saldo totale", final_pivot_df.limit(50))

    printSection("FINE - Job completato")
    println("Lo script ha mostrato pivot base, pivot con valori espliciti, piu metriche, pivot inverso e unpivot.")

    spark.stop()
  }
}
