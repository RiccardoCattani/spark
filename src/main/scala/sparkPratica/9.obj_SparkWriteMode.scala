package sparkPratica

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

// Scopo dello script
// ------------------
// Questo esempio mostra come leggere un file JSON con Spark e come salvarlo
// nuovamente su disco usando diverse modalita' di scrittura.
//
// In Spark, quando si salva un DataFrame, la cartella di destinazione non viene
// trattata come un singolo file ma come una directory di output. Al suo interno
// Spark crea uno o piu file part-*.json, piu eventuali file tecnici come
// _SUCCESS.
//
// La parte piu importante dell'esempio e' il metodo mode(...), che controlla
// cosa deve fare Spark se la cartella di output esiste gia':
//
// - mode("error"):
//   comportamento predefinito. Se la cartella esiste gia', Spark si ferma con
//   errore per evitare di sovrascrivere dati esistenti.
//
// - mode("overwrite"):
//   se la cartella esiste gia', Spark la sostituisce con il nuovo output.
//   Questa modalita' e' comoda negli esercizi, ma va usata con attenzione su
//   dati reali perche' puo cancellare output precedenti.
//
// - mode("ignore"):
//   se la cartella esiste gia', Spark non scrive nulla e non genera errore.
//   E' una modalita' "non overwrite": conserva quello che c'e' gia'.
//
// - mode("append"):
//   se la cartella esiste gia', Spark aggiunge nuovi file part-*.json nella
//   stessa directory. Non modifica i file gia presenti.
//
object obj_SparkWriteMode {
  private def printSection(title: String): Unit = {
    println()
    println("=" * 90)
    println(title)
    println("=" * 90)
  }

  def main(arg: Array[String]): Unit = {
    printSection("AVVIO - Esempio modalita' di scrittura JSON")
    println("Obiettivo: leggere user.json e scriverlo con diverse mode di Spark.")
    println("Input: C:\\repository\\spark\\1.input\\user.json")

    // 1. Configurazione dell'applicazione Spark.
    //
    // SparkConf contiene le impostazioni base dell'applicazione:
    // - setAppName("job1") assegna un nome al job Spark.
    // - setMaster("local[*]") indica che il programma gira in locale usando
    //   tutti i core disponibili della macchina.
    val conf = new SparkConf()
      .setAppName("job1")
      .setMaster("local[*]")

    printSection("1 - Configurazione Spark")
    println("Creo SparkConf con appName=job1 e master=local[*].")
    println("local[*] usa tutti i core disponibili della macchina.")

    // 2. Creazione dello SparkContext.
    //
    // SparkContext e' il punto di ingresso piu basso livello di Spark.
    // In molti esempi moderni si usa direttamente SparkSession, ma qui viene
    // creato anche SparkContext per mostrare la configurazione classica.
    //
    // setLogLevel("ERROR") riduce i messaggi di log, mostrando solo gli errori
    // principali e rendendo piu leggibile l'output dell'esercizio.
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    printSection("2 - Creazione SparkContext")
    println("SparkContext inizializza l'applicazione Spark.")
    println("Il log level ERROR riduce i messaggi non essenziali nel terminale.")

    // 3. Creazione della SparkSession.
    //
    // SparkSession e' il punto di ingresso principale per lavorare con
    // DataFrame e Dataset. Qui viene costruita usando la configurazione creata
    // sopra, cosi SparkSession e SparkContext condividono le stesse opzioni.
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    printSection("3 - Creazione SparkSession")
    println("SparkSession e' il punto di ingresso principale per lavorare con DataFrame.")

    // Import utile quando si lavora con DataFrame e Dataset.
    // In questo esempio non e' indispensabile, ma e' spesso presente per usare
    // sintassi come $"nome_colonna" o conversioni implicite.
    import spark.implicits._

    // 4. Lettura del file JSON di input.
    //
    // Il file user.json e' in formato JSON Lines: ogni riga e' un oggetto JSON
    // completo. Spark legge automaticamente le colonne e ne deduce lo schema.
    val df = spark.read
      .format("json")
      .load("C:\\repository\\spark\\1.input\\user.json")

    printSection("4 - Lettura file JSON")
    println("Spark legge user.json in formato JSON Lines: un oggetto JSON per ogni riga.")
    println("Path input: C:\\repository\\spark\\1.input\\user.json")

    // 5. Controllo del DataFrame letto.
    //
    // printSchema mostra i nomi delle colonne e i tipi riconosciuti da Spark.
    // show(false) mostra il contenuto senza troncare le stringhe lunghe.
    println(s"Numero righe lette: ${df.count()}")
    println("Schema del DataFrame letto:")
    df.printSchema()
    println("Dati letti dal JSON:")
    df.show(false)

    // 6. Scrittura con mode("error").
    //
    // Questa modalita' fallisce se la directory di output esiste gia'.
    // E' utile quando si vuole evitare di sovrascrivere accidentalmente dati
    // gia prodotti. E' anche la modalita' predefinita se non si specifica mode.
    //
    // Il try/catch serve solo per l'esercizio: se la cartella esiste gia',
    // mostriamo il messaggio ma lasciamo proseguire il programma, cosi si puo
    // vedere anche l'esempio successivo con mode("overwrite").
    printSection("5 - Scrittura con mode(\"error\")")
    println("Questa mode prova a scrivere solo se la cartella non esiste gia'.")
    println("Se la cartella esiste, Spark genera errore. Qui lo intercettiamo con try/catch.")
    println("Output: C:\\repository\\spark\\2.output\\example\\json")
    try {
      df.write
        .format("json")
        .mode("error")
        .save("C:\\repository\\spark\\2.output\\example\\json")
    } catch {
      case exception: Exception =>
        println("Scrittura con mode(\"error\") non eseguita.")
        println(s"Motivo: ${exception.getMessage}")
    }

    // 7. Scrittura con mode("overwrite").
    //
    // Questa modalita' sovrascrive la directory di output se esiste gia'.
    // Negli esercizi e' pratica per rilanciare piu volte lo stesso programma
    // senza dover cancellare manualmente la cartella di output.
    printSection("6 - Scrittura con mode(\"overwrite\")")
    println("Questa mode sovrascrive la cartella di output se esiste gia'.")
    println("Output: C:\\repository\\spark\\2.output\\user_json_output")
    df.write
      .format("json")
      .mode("overwrite")
      .save("C:\\repository\\spark\\2.output\\user_json_output")

    // 8. Scrittura con mode("ignore").
    //
    // Questa e' una versione "non overwrite":
    // - se la directory non esiste, Spark scrive i dati;
    // - se la directory esiste gia', Spark non fa nulla;
    // - non viene generato errore.
    //
    // E' utile quando si vuole creare un output solo la prima volta, evitando
    // sia la sovrascrittura sia il blocco del programma.
    printSection("7 - Scrittura con mode(\"ignore\")")
    println("Questa mode scrive solo se la cartella non esiste.")
    println("Se la cartella esiste gia', Spark non fa nulla e non genera errore.")
    println("Output: C:\\repository\\spark\\2.output\\user_json_ignore")
    df.write
      .format("json")
      .mode("ignore")
      .save("C:\\repository\\spark\\2.output\\user_json_ignore")

    // 9. Scrittura con mode("append").
    //
    // Questa modalita' aggiunge nuovi file alla directory di output.
    // Se rilanci il programma piu volte, dentro user_json_append troverai piu
    // file part-*.json, uno o piu per ogni esecuzione.
    //
    // Attenzione: append non aggiorna le righe esistenti. Aggiunge soltanto
    // nuovi dati, quindi rilanciando lo stesso job puoi ottenere duplicati.
    printSection("8 - Scrittura con mode(\"append\")")
    println("Questa mode aggiunge nuovi file part-*.json alla cartella esistente.")
    println("Rilanciando il job piu volte puoi ottenere record duplicati.")
    println("Output: C:\\repository\\spark\\2.output\\user_json_append")
    df.write
      .format("json")
      .mode("append")
      .save("C:\\repository\\spark\\2.output\\user_json_append")

    // 10. Chiusura delle risorse Spark.
    //
    // stop() libera le risorse usate da Spark. In programmi piccoli sembra
    // secondario, ma e' una buona pratica chiudere sempre SparkSession e
    // SparkContext quando il job e' terminato.
    printSection("FINE - Job completato")
    println("Sono stati mostrati gli effetti principali di error, overwrite, ignore e append.")

    spark.stop()
    sc.stop()
  }
}
