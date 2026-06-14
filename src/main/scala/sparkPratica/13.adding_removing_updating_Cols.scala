// sbt "runMain sparkPratica.adding_removing_updating_Cols" *> output_script_13.txt
// Scopo dello script
// ------------------
// Questo script dimostra alcune operazioni di analisi preliminare su un DataFrame
// Spark letto da CSV. L'obiettivo non e' trasformare i dati, ma controllarne la
// struttura e la qualita'.
//
// In particolare lo script:
// - legge un CSV di transazioni bancarie con header e inferenza dello schema (ossia rispettivamente i nomi delle colonne e i tipi vengono dedotti automaticamente da Spark);
// - mostra colonne, schema, numero righe e dati di esempio;
// - calcola statistiche descrittive con describe();
// - conta, per ogni colonna, quanti valori sono null o vuoti.
//
// Serve quindi come esempio di data profiling iniziale prima di applicare
// trasformazioni o pulizia piu' avanzata.
//
package sparkPratica

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Questo script legge un CSV di transazioni bancarie e mostra operazioni utili
// per ispezionare un DataFrame: schema, dati, statistiche descrittive e conteggio
// dei valori null o vuoti per ogni colonna.
object adding_removing_updating_Cols {
  private val MaxRowsToShow = 100

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

  def main(arg: Array[String]): Unit = {
    // Configura Spark in locale e crea lo SparkContext.
    val conf = new SparkConf().setAppName("bank_Trans").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")

    // Crea la SparkSession per leggere il CSV come DataFrame.
    val spark = SparkSession.builder().getOrCreate()

    //IMP Legge il CSV con header "true" e inferSchema "true".
    //
    // option("header", "true") dice a Spark che la prima riga del file non e'
    // un record dati, ma contiene i nomi delle colonne. Per esempio, se la prima
    // riga e':
    // InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country
    //
    // Spark avendo "header true" usera' questi valori come nomi delle colonne del DataFrame.
    // Senza header=true, Spark tratterebbe quella riga come un normale dato e
    // chiamerebbe le colonne con nomi generici come _c0, _c1, _c2.
    //
    // option("inferSchema", "true") chiede a Spark di analizzare i valori del CSV
    // per provare a dedurre automaticamente il tipo di ogni colonna.
    // Per esempio:
    // - Quantity potrebbe diventare IntegerType;
    // - UnitPrice potrebbe diventare DoubleType;
    // - Description e Country rimangono StringType;
    // - date e timestamp possono essere riconosciuti se il formato e' compatibile.
    //
    // Se non usiamo inferSchema, Spark legge normalmente tutte le colonne CSV come
    // stringhe. Questo e' piu veloce in lettura, ma poi per fare calcoli numerici,
    // confronti su date o aggregazioni corrette dovremmo convertire i tipi a mano.
    //
    // Nota pratica: inferSchema e' comodo negli esercizi e nelle analisi rapide,
    // ma su file grandi puo' costare tempo perche' Spark deve ispezionare i dati.
    // In produzione spesso e' meglio definire uno schema esplicito con StructType,
    // cosi i tipi sono controllati e non dipendono da come Spark interpreta il file.
    //
    // Esempio risultato dopo la lettura:
    //
    // TransactionID | CustomerID | CustomerDOB | CustGender | CustLocation | CustAccountBalance | TransactionDate | TransactionTime | TransactionAmount (INR)
    // T1            | C5841053   | 10/1/94     | F          | JAMSHEDPUR   | 17819.05           | 2/8/16          | 143207          | 25.0
    val read_csv_df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("C:\\repository\\spark\\1.input\\bank_transactions.csv")
      .persist()

    showDataFrameDetails("CSV transazioni bancarie letto con inferSchema", read_csv_df)

    // Selezione delle colonne richieste.
    // select crea un nuovo DataFrame contenente solo le colonne indicate rispetto a quelle originali.
    // Il DataFrame originale read_csv_df non viene modificato: i DataFrame Spark
    // sono immutabili, quindi ogni trasformazione restituisce un nuovo DataFrame.
    //
    // Quindi manterremo solo queste colonne:
    // - TransactionID: identificativo della transazione;
    // - CustomerID: identificativo del cliente;
    // - CustGender: genere del cliente;
    // - CustAccountBalance: saldo del conto del cliente;
    // - TransactionDate: data della transazione;
    // - TransactionAmount (INR): importo della transazione in rupie indiane.
    //
    // alias("Transaction_Amount") rinomina la colonna nell'output.
    // Questo e' utile per togliere spazi, parentesi e caratteri speciali dal nome
    // originale "TransactionAmount (INR)", rendendo la colonna piu comoda da usare
    // nelle trasformazioni successive.
    //
    // Prima, read_csv_df:
    //
    // TransactionID | CustomerID | CustomerDOB | CustGender | CustLocation | CustAccountBalance | TransactionDate | TransactionTime | TransactionAmount (INR)
    // T1            | C5841053   | 10/1/94     | F          | JAMSHEDPUR   | 17819.05           | 2/8/16          | 143207          | 25.0
    //
    // Dopo, select_df:
    //
    // TransactionID | CustomerID | CustGender | CustAccountBalance | TransactionDate | Transaction_Amount
    // T1            | C5841053   | F          | 17819.05           | 2/8/16          | 25.0
    val select_df = read_csv_df.select( // Seleziona le colonne richieste e rinomina TransactionAmount (INR)
      col("TransactionID"),
      col("CustomerID"),
      col("CustGender"),
      col("CustAccountBalance"),
      col("TransactionDate"),
      col("TransactionAmount (INR)").alias("Transaction_Amount") // Rinomina la colonna con alias
    )

    showDataFrameDetails("Selezione colonne richieste con alias su TransactionAmount", select_df)

    // Selezione delle colonne con selectExpr.
    //
    // selectExpr fa un lavoro simile a select, ma riceve espressioni scritte come
    // stringhe SQL. E' utile quando vogliamo selezionare colonne, rinominarle,
    // fare calcoli o applicare piccole espressioni direttamente nella select.
    //
    // Per esempio, questa riga:
    // "Transaction_Amount"
    //
    // seleziona la colonna gia' rinominata nel DataFrame select_df.
    // Se volessimo rinominare direttamente da una colonna con spazi o parentesi,
    // potremmo usare i backtick SQL:
    //
    // "`TransactionAmount (INR)` as Transaction_Amount"
    //
    // In questo caso partiamo da select_df, quindi la colonna si chiama gia'
    // Transaction_Amount grazie all'alias applicato sopra.
    //
    // Risultato di select_df_expr:
    //
    // TransactionID | CustomerID | CustAccountBalance | Transaction_Amount
    // T1            | C5841053   | 17819.05           | 25.0
    printSection("Selezione colonne usando selectExpr") 
    val select_df_expr = select_df.selectExpr(
      "TransactionID",
      "CustomerID",
      "CustAccountBalance",
      "Transaction_Amount"
    )

    showDataFrameDetails("Selezione colonne con selectExpr", select_df_expr)

    // Variante: selectExpr direttamente dal DataFrame originale.
    //
    // Quando una colonna contiene spazi, parentesi o altri caratteri speciali,
    // in una espressione SQL conviene racchiuderla tra backtick.
    //
    // Qui la colonna originale si chiama:
    //
    // TransactionAmount (INR)
    //
    // Per rinominarla direttamente con selectExpr scriviamo:
    //
    // `TransactionAmount (INR)` as Transaction_Amount
    //
    // In questo modo otteniamo lo stesso alias creato prima con:
    // col("TransactionAmount (INR)").alias("Transaction_Amount")
    //
    // Risultato di select_df_expr_direct:
    //
    // TransactionID | CustomerID | CustGender | CustAccountBalance | TransactionDate | Transaction_Amount
    // T1            | C5841053   | F          | 17819.05           | 2/8/16          | 25.0
    printSection("selectExpr diretto dal DataFrame originale con backtick e alias")
    val select_df_expr_direct = read_csv_df.selectExpr(
      "TransactionID",
      "CustomerID",
      "CustGender",
      "CustAccountBalance",
      "TransactionDate",
      "`TransactionAmount (INR)` as Transaction_Amount"
    )

    showDataFrameDetails("selectExpr diretto con alias su TransactionAmount", select_df_expr_direct)

    // selectExpr puo' anche eseguire funzioni SQL sulle colonne.
    //
    // split(TransactionDate, '/')[2] divide la data usando "/" come separatore.
    // Se TransactionDate ha un formato come "2/8/16", split produce:
    //
    // indice 0 -> 2
    // indice 1 -> 8
    // indice 2 -> 16
    //
    // Con [2] prendiamo quindi il terzo pezzo, cioe' l'anno nel formato presente
    // nel file. In questo dataset l'anno e' scritto con due cifre.
    // alias con "as Transaction_Year" assegna un nome chiaro alla colonna calcolata.
    //
    // Prima:
    //
    // TransactionDate
    // 2/8/16
    //
    // Dopo l'espressione split(TransactionDate, '/')[2]:
    //
    // Transaction_Year
    // 16
    //
    // Risultato di select_df_expr_with_year:
    //
    // TransactionID | CustomerID | CustGender | CustAccountBalance | Transaction_Year | Transaction_Amount
    // T1            | C5841053   | F          | 17819.05           | 16               | 25.0
    printSection("Selezione colonne con selectExpr e split della data")
    val select_df_expr_with_year = select_df.selectExpr(
      "TransactionID",
      "CustomerID",
      "CustGender",
      "CustAccountBalance",
      "split(TransactionDate, '/')[2] as Transaction_Year",
      "Transaction_Amount"
    )

    showDataFrameDetails("selectExpr con estrazione anno da TransactionDate", select_df_expr_with_year)

    // Aggiunta di nuove colonne con withColumn.
    //
    // withColumn crea un nuovo DataFrame aggiungendo una colonna oppure
    // sostituendo una colonna esistente con lo stesso nome.
    //
    // Qui aggiungiamo tre colonne:
    // - Transaction_Year: anno estratto da TransactionDate;
    // - Currency: valore costante "INR", aggiunto con lit;
    // - Amount_Bucket: classificazione semplice dell'importo.
    //
    // when(...).otherwise(...) funziona come un IF:
    // se la condizione e' vera assegna un valore, altrimenti ne assegna un altro.
    //
    // Prima, select_df:
    //
    // TransactionID | TransactionDate | Transaction_Amount
    // T1            | 2/8/16          | 25.0
    //
    // Dopo, added_cols_df:
    //
    // TransactionID | TransactionDate | Transaction_Amount | Transaction_Year | Currency | Amount_Bucket
    // T1            | 2/8/16          | 25.0               | 16               | INR      | LOW
    printSection("Aggiunta colonne con withColumn")
    val added_cols_df = select_df
      .withColumn("Transaction_Year", split(col("TransactionDate"), "/")(2))
      .withColumn("Currency", lit("INR"))
      .withColumn(
        "Amount_Bucket",
        when(col("Transaction_Amount") >= 10000, "HIGH")
          .when(col("Transaction_Amount") >= 1000, "MEDIUM")
          .otherwise("LOW")
      )

    showDataFrameDetails("DataFrame con colonne aggiunte", added_cols_df)

    // Aggiornamento di una colonna esistente con withColumn.
    // Se withColumn usa il nome di una colonna gia' presente, Spark non aggiunge
    // una seconda colonna: sostituisce quella esistente nel nuovo DataFrame.
    // Qui aggiorniamo Transaction_Amount convertendola esplicitamente in Double.
    // Questo e' utile quando vogliamo essere sicuri che la colonna sia numerica
    // prima di fare calcoli, confronti o aggregazioni.
    //
    // Prima:
    //
    // Transaction_Amount
    // 25.0
    //
    // Dopo:
    //
    // Transaction_Amount
    // 25.0
    //
    // Il valore a video sembra uguale, ma nello schema la colonna viene forzata
    // a double. La differenza si vede con printSchema().
    printSection("Aggiornamento colonna esistente con withColumn")
    val updated_amount_df = added_cols_df
      .withColumn("Transaction_Amount", col("Transaction_Amount").cast("double"))

    showDataFrameDetails("DataFrame con Transaction_Amount convertita in double", updated_amount_df)

    // Rinomina di una colonna con withColumnRenamed.
    //
    // withColumnRenamed cambia solo il nome della colonna, non i valori.
    // Qui rendiamo piu descrittivo il nome CustGender.
    //
    // Prima:
    //
    // CustGender
    // F
    //
    // Dopo:
    //
    // Customer_Gender
    // F
    printSection("Rinomina colonna con withColumnRenamed")
    val renamed_cols_df = updated_amount_df
      .withColumnRenamed("CustGender", "Customer_Gender")

    showDataFrameDetails("DataFrame con CustGender rinominata", renamed_cols_df)

    // Rimozione di colonne con drop.
    //
    // drop restituisce un nuovo DataFrame senza le colonne indicate.
    // Qui rimuoviamo TransactionDate perche' ormai abbiamo gia' estratto
    // Transaction_Year, e rimuoviamo Currency solo per mostrare come eliminare
    // piu colonne nella stessa operazione.
    //
    // Prima, renamed_cols_df:
    //
    // TransactionID | Customer_Gender | TransactionDate | Transaction_Amount | Transaction_Year | Currency | Amount_Bucket
    // T1            | F               | 2/8/16          | 25.0               | 16               | INR      | LOW
    //
    // Dopo, dropped_cols_df:
    //
    // TransactionID | Customer_Gender | Transaction_Amount | Transaction_Year | Amount_Bucket
    // T1            | F               | 25.0               | 16               | LOW
    printSection("Rimozione colonne con drop")
    val dropped_cols_df = renamed_cols_df
      .drop("TransactionDate", "Currency")

    showDataFrameDetails("DataFrame dopo rimozione colonne", dropped_cols_df)

    // describe calcola statistiche base come count, mean, stddev, min e max.
    //
    // Risultato atteso: una tabella di riepilogo con righe come count, mean,
    // stddev, min e max. Esempio su alcune colonne:
    //
    // summary | TransactionID | CustomerID | CustAccountBalance | TransactionAmount (INR)
    // count   | 1048567       | 1048567    | 1046198            | 1048567
    // mean    | NULL          | NULL       | 115403.54...       | 1574.33...
    // min     | T1            | C1010011   | 0.0                | 0.0
    // max     | T999999       | C9099956   | 1.150354951E8      | 1560034.99
    printSection("Statistiche descrittive colonne numeriche/stringa")
    read_csv_df.describe().show(MaxRowsToShow, truncate = false)

    // Per ogni colonna conta i valori null o stringhe vuote.
    //
    // Risultato atteso: una sola riga, con il numero di valori mancanti per colonna.
    // Nell'esecuzione su questo file si ottiene per esempio:
    //
    // TransactionID | CustomerID | CustomerDOB | CustGender | CustLocation | CustAccountBalance | TransactionDate | TransactionTime | TransactionAmount (INR)
    // 0             | 0          | 0           | 1100       | 151          | 2369               | 0               | 0               | 0
    printSection("Conteggio valori null per colonna")
    val nullCounts = read_csv_df.columns.map { columnName =>
      sum(when(col(columnName).isNull || trim(col(columnName).cast("string")) === "", 1).otherwise(0)).alias(columnName)
    }
    read_csv_df.select(nullCounts: _*).show(truncate = false)

    // Chiude SparkSession e SparkContext.
    spark.stop()
    sc.stop()
  }
}
