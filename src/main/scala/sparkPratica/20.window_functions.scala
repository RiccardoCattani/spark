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
    // Questa riga definisce "come" Spark deve guardare le righe quando
    // applichiamo una funzione di finestra.
    //
    // Importante:
    // windowSpec da sola non produce ancora nessuna colonna nuova.
    // E' solo una regola. Il calcolo avviene dopo, quando scriviamo:
    //
    // row_number().over(windowSpec)
    // rank().over(windowSpec)
    // lag(...).over(windowSpec)
    //
    // Quindi possiamo leggere windowSpec cosi':
    //
    // "Per ogni CustomerID, guarda solo le righe di quel cliente e ordinalle
    //  per CustAccountBalance crescente."
    //
    // partitionBy("CustomerID")
    //   divide logicamente il DataFrame in gruppi separati, uno per ogni
    //   cliente.
    //
    //   Non e' un groupBy classico:
    //   - groupBy riduce le righe e restituisce una riga per gruppo;
    //   - Window.partitionBy mantiene tutte le righe originali.
    //
    //   La differenza e' fondamentale:
    //
    //   groupBy("CustomerID").agg(sum(...))
    //     produce una riga finale per ogni CustomerID.
    //
    //   Window.partitionBy("CustomerID")
    //     mantiene ogni transazione, ma permette a ogni riga di "vedere" le
    //     altre righe dello stesso cliente.
    //
    // orderBy("CustAccountBalance")
    //   dentro ogni cliente, ordina le righe per saldo crescente.
    //
    //   L'ordinamento avviene dentro ogni partizione logica, non sull'intero
    //   DataFrame. Quindi il cliente C1119249 viene ordinato separatamente dal
    //   cliente C222, C333, ecc.
    //
    // Esempio logico:
    //
    // Prima possiamo avere righe mischiate:
    //
    // CustomerID | CustAccountBalance
    // C222      | 900.00
    // C1119249  | 24911.32
    // C1119249  | 6520.75
    // C333      | 100.00
    // C1119249  | 16406.51
    //
    // Dopo partitionBy("CustomerID"), Spark ragiona come se avesse gruppi
    // separati:
    //
    // Cliente C1119249:
    // 24911.32
    // 6520.75
    // 16406.51
    //
    // Cliente C222:
    // 900.00
    //
    // Cliente C333:
    // 100.00
    //
    // Dopo orderBy("CustAccountBalance"), dentro C1119249 le righe diventano:
    //
    // CustomerID | CustAccountBalance
    // C1119249  | 6520.75
    // C1119249  | 16406.51
    // C1119249  | 24911.32
    //
    // La finestra del cliente C1119249 contiene solo quelle righe, ordinate per
    // CustAccountBalance.
    //
    // A questo punto le funzioni successive possono usare questa finestra.
    //
    // row_number() dara':
    //
    // CustomerID | CustAccountBalance | row_num
    // C1119249  | 6520.75            | 1
    // C1119249  | 16406.51           | 2
    // C1119249  | 24911.32           | 3
    //
    // lag("CustAccountBalance", 1) dara':
    //
    // CustomerID | CustAccountBalance | lag
    // C1119249  | 6520.75            | null
    // C1119249  | 16406.51           | 6520.75
    // C1119249  | 24911.32           | 16406.51
    //
    // lead("CustAccountBalance", 1) fara' il contrario: prende il saldo della
    // riga successiva dentro lo stesso CustomerID.
    val windowSpec = Window
      .partitionBy("CustomerID")
      .orderBy(col("CustAccountBalance").asc)

    printSection("3 - Window specification")
    println("Window.partitionBy(\"CustomerID\").orderBy(\"CustAccountBalance\") crea una finestra per cliente ordinata per saldo.")

    // row_number.
    //
    // row_number() assegna un numero progressivo a ogni riga dentro la finestra.
    // Il conteggio riparte da 1 per ogni CustomerID, perche' la finestra e'
    // stata definita con partitionBy("CustomerID").
    //
    // Quindi Spark non numera tutte le righe del DataFrame da 1 a N.
    // Numera separatamente le righe di ogni cliente.
    //
    // Se un cliente ha 3 righe ordinate per saldo, otteniamo:
    //
    // CustomerID | CustAccountBalance | row_num
    // C1119249  | 6520.75            | 1
    // C1119249  | 16406.51           | 2
    // C1119249  | 24911.32           | 3
    //
    // row_number non lascia buchi: 1, 2, 3, 4...
    //
    // Attenzione ai pari merito:
    // se due righe hanno lo stesso CustAccountBalance, row_number assegna
    // comunque due numeri diversi. L'ordine tra valori uguali puo' non essere
    // stabile se non aggiungiamo un secondo criterio di ordinamento, per esempio
    // orderBy(col("CustAccountBalance"), col("TransactionID")).
    //
    // Uso tipico:
    // - prendere la prima riga per ogni cliente;
    // - tenere le top N righe per ogni gruppo;
    // - creare un ordinamento progressivo dentro una categoria.
    val row_number_df = df
      .select(col("CustomerID"), col("CustAccountBalance"))
      .withColumn("row_num", row_number().over(windowSpec))

    showDataFrameDetails("4 - row_number per CustomerID", row_number_df.orderBy("CustomerID", "row_num").limit(100))

    // rank.
    //
    // rank() assegna una posizione dentro la finestra.
    // Se ci sono valori uguali, assegna lo stesso rank e lascia un buco.
    //
    // rank ragiona come una classifica sportiva:
    // se due righe sono prime a pari merito, la riga successiva non e' seconda,
    // ma terza.
    //
    // Esempio:
    //
    // saldo | rank | spiegazione
    // 100   | 1    | prima posizione
    // 100   | 1    | pari merito con la prima
    // 200   | 3    | terza posizione, perche' ci sono due righe prima
    //
    // Il valore 200 prende rank 3 perche' due righe sono gia' al primo posto.
    //
    // Uso tipico:
    // - classifiche dove i pari merito devono avere la stessa posizione;
    // - analisi in cui il "buco" nella classifica ha significato;
    // - ordinamenti tipo: 1, 1, 3, 4 oppure 1, 2, 2, 4.
    val rank_df = df
      .select(col("CustomerID"), col("CustAccountBalance"))
      .withColumn("rank", rank().over(windowSpec))

    showDataFrameDetails("5 - rank per CustomerID", rank_df.orderBy("CustomerID", "rank").limit(100))

    // dense_rank.
    //
    // dense_rank() e' simile a rank, ma non lascia buchi.
    //
    // Anche dense_rank assegna lo stesso valore ai pari merito.
    // La differenza e' che la posizione successiva continua in modo compatto.
    //
    // Esempio:
    //
    // saldo | rank | dense_rank
    // 100   | 1    | 1
    // 100   | 1    | 1
    // 200   | 3    | 2
    // 300   | 4    | 3
    //
    // Il valore 200 prende dense_rank 2, non 3.
    //
    // Uso tipico:
    // - creare fasce compatte di valori;
    // - assegnare una posizione progressiva ai valori distinti;
    // - evitare buchi quando interessa solo l'ordine dei valori diversi.
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
    //
    // Esempio con valori uguali:
    //
    // saldo | row_number | rank | dense_rank
    // 100   | 1          | 1    | 1
    // 100   | 2          | 1    | 1
    // 200   | 3          | 3    | 2
    // 300   | 4          | 4    | 3
    //
    // In questo script l'ordinamento e' per CustAccountBalance crescente.
    // Se volessimo classificare dal saldo piu alto al piu basso, useremmo:
    //
    // orderBy(col("CustAccountBalance").desc)
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
    // Formula concettuale:
    //
    // percent_rank = (rank - 1) / (numero_righe_nella_finestra - 1)
    //
    // Per il primo record della finestra di solito vale 0.0.
    // Per l'ultimo record tende a 1.0.
    //
    // Serve quando vogliamo capire la posizione percentuale di una riga dentro
    // il gruppo, non solo la posizione intera.
    //
    // Esempio con 4 righe senza pari merito:
    //
    // saldo | rank | percent_rank
    // 100   | 1    | 0.0
    // 200   | 2    | 0.3333
    // 300   | 3    | 0.6667
    // 400   | 4    | 1.0
    //
    // Interpretazione:
    // - 0.0 indica la prima posizione nella finestra;
    // - 1.0 indica l'ultima posizione;
    // - valori intermedi indicano quanto la riga e' avanzata nell'ordinamento.
    //
    // Nota:
    // percent_rank usa rank, quindi i pari merito influenzano anche il valore
    // percentuale.
    val percent_rank_df = ranking_df
      .withColumn("percent_rank", percent_rank().over(windowSpec))

    showDataFrameDetails("8 - percent_rank per CustomerID", percent_rank_df.orderBy("CustomerID", "row_num").limit(100))

    // lag.
    //
    // lag(col, 1) prende il valore della riga precedente dentro la stessa
    // finestra.
    //
    // Nel nostro caso:
    //
    // lag(col("CustAccountBalance"), 1)
    //
    // significa:
    //
    // "Per questa riga, prendi il CustAccountBalance della riga precedente
    //  dello stesso CustomerID, secondo l'ordine definito da windowSpec."
    //
    // Esempio:
    //
    // CustomerID | CustAccountBalance | lag
    // C1119249  | 6520.75            | null
    // C1119249  | 16406.51           | 6520.75
    // C1119249  | 24911.32           | 16406.51
    //
    // La prima riga non ha una riga precedente, quindi lag restituisce null.
    //
    // Uso tipico:
    // - confrontare una riga con quella precedente;
    // - calcolare una differenza rispetto al valore precedente;
    // - analizzare sequenze temporali, ad esempio transazione corrente contro
    //   transazione precedente.
    //
    // Esempio di calcolo possibile:
    //
    // withColumn("differenza",
    //   col("CustAccountBalance") - lag(col("CustAccountBalance"), 1).over(windowSpec)
    // )
    val lag_df = percent_rank_df
      .withColumn("lag", lag(col("CustAccountBalance"), 1).over(windowSpec))

    showDataFrameDetails("9 - lag: saldo precedente nello stesso CustomerID", lag_df.orderBy("CustomerID", "row_num").limit(100))

    // lead.
    //
    // lead(col, 1) prende il valore della riga successiva dentro la stessa
    // finestra.
    //
    // Nel nostro caso:
    //
    // lead(col("CustAccountBalance"), 1)
    //
    // significa:
    //
    // "Per questa riga, prendi il CustAccountBalance della riga successiva
    //  dello stesso CustomerID, secondo l'ordine definito da windowSpec."
    //
    // Esempio:
    //
    // CustomerID | CustAccountBalance | lead
    // C1119249  | 6520.75            | 16406.51
    // C1119249  | 16406.51           | 24911.32
    // C1119249  | 24911.32           | null
    //
    // L'ultima riga non ha una riga successiva, quindi lead restituisce null.
    //
    // Uso tipico:
    // - confrontare una riga con quella successiva;
    // - calcolare la distanza dal prossimo valore;
    // - vedere cosa succede "dopo" una riga senza fare self join.
    //
    // Differenza pratica:
    // - lag guarda indietro;
    // - lead guarda avanti.
    val lag_lead_df = lag_df
      .withColumn("lead", lead(col("CustAccountBalance"), 1).over(windowSpec))

    showDataFrameDetails("10 - lag e lead", lag_lead_df.orderBy("CustomerID", "row_num").limit(100))

    // Focus su un cliente.
    //
    // Nel video viene filtrato un CustomerID specifico per vedere bene le righe
    // della stessa finestra. Usiamo un cliente presente nel dataset di esempio.
    //
    // Qui la tabella e' piu leggibile perche' mostra solo le righe di C1119249.
    //
    // Questo passaggio non e' necessario per il calcolo.
    // Serve solo per osservare meglio il risultato:
    //
    // - row_num deve crescere da 1 in poi;
    // - rank e dense_rank devono rispettare l'ordinamento del saldo;
    // - lag deve mostrare il saldo precedente;
    // - lead deve mostrare il saldo successivo.
    //
    // In pratica stiamo isolando una singola finestra per controllare
    // visivamente che le funzioni si comportino come previsto.
    val customer_focus_df = lag_lead_df
      .where(col("CustomerID") === "C1119249")
      .orderBy(col("row_num"))

    showDataFrameDetails("11 - Focus su CustomerID = C1119249", customer_focus_df)

    // Esempio con piu colonne di contesto.
    //
    // La finestra resta la stessa, ma manteniamo anche TransactionID,
    // TransactionDate e CustLocation per capire da quale transazione arriva ogni
    // saldo.
    //
    // Nei passaggi precedenti abbiamo selezionato quasi solo CustomerID e
    // CustAccountBalance per rendere gli esempi piu semplici.
    //
    // Qui invece ricostruiamo una vista piu utile in un caso reale:
    //
    // TransactionID
    //   identifica la transazione specifica.
    //
    // CustLocation
    //   aggiunge il luogo associato al cliente/transazione.
    //
    // TransactionDate
    //   aiuta a leggere il risultato con un contesto temporale.
    //
    // Transaction_Amount
    //   permette di confrontare saldo del conto e importo della transazione.
    //
    // Le colonne calcolate con la finestra restano identiche come logica:
    // Spark continua a partizionare per CustomerID e ordinare per
    // CustAccountBalance. Cambia solo il numero di colonne mostrate in output.
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
