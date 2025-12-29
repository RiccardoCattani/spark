# DIFFERENZA TRA HIVE, IMPALA E SQOOP
(E TRA DATA WAREHOUSE E MOTORE SQL)

## DATA LAKE VS DATA WAREHOUSE

**Data Lake**  (Es.Hadoop, Amazon S3, Azure Data Lake Storage)
- Brevemente: Possiede fisicamente i dati (Grezzi, strutturati e non strutturati)
- Un data lake è un sistema di archiviazione che raccoglie grandi quantitÃ  di dati grezzi, strutturati e non strutturati, provenienti da fonti diverse.
- I dati vengono memorizzati cosÃ¬ come sono (schema-on-read), senza una struttura predefinita.
- Ãˆ pensato per la scalabilitÃ , la flessibilitÃ  e lâ€™analisi di dati eterogenei (log, immagini, file, dati IoT, ecc.).
- Tipicamente utilizza storage distribuito come HDFS, S3, ADLS.

**Data Warehouse** (Es. Hive, Snowflake, BigQuery, Redshift)
- Brevemente: governa, organizza, cataloga e struttura i dati
- Un data warehouse Ã¨ un sistema che organizza, struttura e governa i dati ma non li possiede
- I dati sono strutturati in tabelle e schemi (schema-on-write), con qualitÃ  e coerenza garantite.
- il data warehouse non possiede fisicamente i dati: governa, organizza, cataloga e struttura i dati, ma questi risiedono nello storage sottostante (come HDFS, S3, ADLS). Il data warehouse gestisce metadati, schemi, tabelle e processi ETL, mentre i file con le righe dei dati sono nello storage. Alcuni data warehouse cloud integrano anche lo storage, ma la logica resta: il data warehouse governa, lo storage possiede i dati.
- Ottimizzato per query analitiche, BI e reporting.
- Gestisce metadati, sicurezza, storico e processi ETL

Nel dettaglio:
a) Gestione dei metadati: Tiene traccia delle informazioni che descrivono i dati (schema delle tabelle, tipi di colonne, partizioni, permessi, ecc.), facilitando la comprensione, la ricerca e lâ€™utilizzo dei dati stessi. Si ricorda che i dati sono le informazioni che vuoi analizzare o conservare (es. le righe di una tabella: nomi, numeri, date, transazioni, ecc.), mentre i metadati sono sono le informazioni che descrivono i dati stessi (es. schema della tabella, nomi e tipi delle colonne, partizioni, permessi, percorso dei file, definizioni dei campi).
b) Sicurezza: Permette di definire chi puÃ² accedere a quali dati, impostando permessi e ruoli per utenti e gruppi, garantendo la protezione delle informazioni sensibili.
c) Storico: Consente di mantenere versioni storiche dei dati, tracciare le modifiche e gestire lo storico delle tabelle, utile per audit, analisi temporali e ripristino.
Processi ETL (Extract, Transform, Load): Supporta e governa i processi di estrazione, trasformazione e caricamento dei dati, assicurando che i dati siano puliti, coerenti e pronti per lâ€™analisi.

### Differenze principali

| Caratteristica         | Data Lake                        | Data Warehouse                  |
|------------------------|----------------------------------|---------------------------------|
| Tipo di dati           | Grezzi, strutturati e non        | Strutturati                     |
| Schema                 | On-read (al momento della query) | On-write (alla scrittura)       |
| Governance             | Limitata                         | Elevata                         |
| Performance query      | Variabile                        | Ottimizzata per analisi         |
| Costo                  | Basso (storage scalabile)        | PiÃ¹ alto (ottimizzato, gestito) |
| Utenti tipici          | Data scientist, ingegneri dati   | Analisti, business, BI          |
| Esempi                 | Hadoop, S3, ADLS                 | Hive, Snowflake, BigQuery       |

**In sintesi:**
Il data warehouse governa i dati (schema, tabelle, partizioni, sicurezza, qualitÃ ) e li rende interrogabili, ma i file fisici stanno nello storage sottostante (es. HDFS/S3/ADLS). Il metastore/catalogo sa dove sono i file e come interpretarli; il motore SQL legge/scrive quei file. Eccezione: alcuni DW cloud (Snowflake, BigQuery) integrano anche lo storage, ma il concetto resta: il layer warehouse organizza e governa, lo storage contiene fisicamente i dati.

1) DATA WAREHOUSE VS MOTORE SQL

a) Data Warehouse
- Governa (Ma non possiede) e organizza i dati analitici (Le righe, ossia i dati fisici, risiedono nei file su HDFS/S3/ADLS. Hive/Impala leggono e scrivono quei file, e il metastore tiene il catalogo (tabelle, schemi, partizioni, permessi) 
a) Definisce schemi, tabelle, partizioni e storico
b) Gestisce metadati, sicurezza e processi batch / ETL
- PuÃ² utilizzare uno o piÃ¹ motori SQL:
Tra i quali: Hive, Snowflake, BigQuery
Attenzione: Hive Ã¨ sia un data warehouse che un motore SQL

**Differenza tra File System (es. HDFS) e Data Warehouse**
- Un file system come HDFS si occupa solo di memorizzare file e cartelle, senza struttura o regole sui dati.
- Un data warehouse, invece, organizza i dati in tabelle, schemi e partizioni, gestisce metadati, sicurezza, storico e processi di caricamento/analisi. Fornisce strumenti per interrogare e governare i dati.
Per essere precisi, nel mondo Hive/Impala: il datawharehouse governa (schemi, tabelle, partizioni, metadati, sicurezza) ma le righe vivono nei file su HDFS/S3/ADLS. Il catalogo dice dove sono i file e come interpretarli; i motori SQL leggono/scrivono file nuovi, non tengono le righe dentro il metastore.
Eccezione: alcuni data warehouse cloud (Snowflake, BigQuery) integrano anche lo storage fisico, ma la logica resta la stessa: catalogo/metadati + motore; i dati stanno comunque in uno storage sottostante.

**Schema rapido (cosa fa chi)**
- HDFS: memorizza i file/blocchi, ossia i byte che contengono le righe ma senza conoscere schema o concetto di riga
- Data Warehouse: contiene i metadati e puntatori, definisce schemi, tabelle, partizioni, governa metadati, sicurezza e processi ETL/batch.
- Motore SQL (es. Impala): interroga i dati giÃ  memorizzati, restituisce risultati; non governa i metadati.

**Box rapido: tabelle vs metadati vs dati**
- Tabelle: oggetti logici nel metastore; nome, colonne/tipi, partizioni, formato, path, ma non le righe.
- Metadati: informazioni descrittive (puntatori, schema, tipi, partizioni, permessi, path, formato, statistiche) usate dai motori per trovare e interpretare i file.
- Dati (righe): contenuto reale nei file (Parquet/ORC/CSV...) su HDFS/S3/ADLS; HDFS vede solo byte, non conosce le righe.
Esempio: la tabella Hive "vendite" ha metadati che dicono "colonne: data DATE, importo DECIMAL; formato: Parquet; path: hdfs:///data/vendite/; partizione: anno=2024". I file Parquet in quel path contengono le righe; il metastore non le conserva, ma indica dove sono e come leggerle.

**Significato letterale di "warehouse"**
- "Warehouse" in inglese significa "magazzino". Un data warehouse e quindi un "magazzino di dati", cioe un sistema che raccoglie, organizza e conserva grandi quantita di dati per analisi e reportistica.

b) Motore SQL
- Esegue query SQL
- Calcola e restituisce risultati
- Non governa i dati
- Dipende da uno storage o da un Data Warehouse, come tra gli altri:
Impala, Spark SQL, Trino

One-liner:
Il Data Warehouse governa i dati, il motore SQL li interroga.

--------------------------------------------------


2) APACHE HIVE

Cos’è:
Hive è un Data Warehouse e un SQL-on-Hadoop.

Cosa fa:
- Definisce e governa tabelle, schemi e partizioni
- Gestisce i metadati tramite Hive Metastore
- Permette di interrogare dati su HDFS e object storage (S3, ADLS)
- Usa HiveQL (simile a SQL)
- Traduce le query in MapReduce, Tez o Spark
- Non fornisce query "interattive" nel senso di bassa latenza 

Caratteristiche:
- Orientato a elaborazioni batch (significa che il sistema è progettato per eseguire lavorazioni in lotti: carica ed elabora grandi volumi di dati in job programmati (es. notturni), con più fasi sequenziali e tempi di avvio non istantanei. È ottimizzato per throughput e scalabilità, non per risposte a bassa latenza in tempo reale.)
- Query generalmente lente (minuti), ma altamente scalabili
- Adatto a ETL, preparazione dati e analisi storiche

Come esegue le query (batch MapReduce/Tez/Spark):
- Ogni query scatena piu' fasi (map/shuffle/reduce) con avvio di container YARN, allocazione risorse e pianificazione: solo lo startup pesa secondi o decine di secondi per fase.
- I dati sono letti da HDFS in blocchi e spesso riscritti su disco tra una fase e l'altra (spill/shuffle), quindi molta I/O e serializzazione/deserializzazione.
- L'esecuzione e' pensata per throughput e fault tolerance: se un task fallisce si riavvia; questo aggiunge overhead ma consente di sfruttare cluster grandi.
- La parallelizzazione avviene distribuendo scansioni e shuffle su molti nodi: piu' nodi significano piu' throughput su dati massivi, ma la latenza per singola query resta alta.
- Con formati colonnari, partizionamento e predicate pushdown la latenza migliora, ma il modello resta batch e non low-latency ("Batch e non low-latency" significa che ogni query ha startup e shuffle su piu' fasi, quindi tempi tipicamente di secondi/minuti; per latenza bassa servono motori MPP in-memory o modalita' Hive LLAP).
- Confronto rapido: Impala/Presto/Trino sono motori MPP in-memory e long-running con latenze di secondi, meno robusti per job lunghi; Hive e' piu' lento ma molto scalabile e resistente su volumi enormi.
   
Quando usarlo:
- Elaborazioni pesanti (grandi join/aggregazioni/dedupliche con shuffle massivi, molti task e possibili spill su disco; serve throughput e tolleranza ai fault piu' che latenza)
- ETL complessi (pipeline con molte fasi: join/aggregazioni/dedupliche/arricchimenti/controlli qualita'/partizionamento su grandi volumi, dove contano throughput e scalabilita' piu' della latenza)
- Analisi su grandi volumi di dati
- Non per interrogazioni real-time


--------------------------------------------------


3) APACHE IMPALA

Cos’è:
Impala è un motore SQL MPP (Massively Parallel Processing).

Cosa fa:
- Interroga gli stessi dati di Hive
- Usa gli stessi file su HDFS / S3
- Usa gli stessi metadati (Hive Metastore)
- Fornisce SQL interattivo con bassa latenza

Caratteristiche:
- Query molto veloci (secondi o millisecondi)
- Esecuzione in memoria
- Nessun uso di MapReduce

Quando usarlo:
- BI
- Dashboard
- Analisi esplorativa
- Non per ETL complessi o batch notturni: Impala è un motore in-memoria con esecuzione interattiva, ottimizzato per query veloci e puntuali. Non è adatto per:
  a)ETL complessi**: richiedono elaborazioni pesanti con join massicci, aggregazioni, dedupliche, spill su disco e fault tolerance. Impala ha limiti di memoria per nodo e non gestisce bene i dati che non stanno in RAM; Hive con MapReduce/Tez gestisce meglio volumi enormi spillando su disco e riprovando task falliti.
  b)Batch notturni**: sono job lunghi che girano offline; l'architettura in-memoria di Impala non è pensata per processi prolungati, e i dati devono stare in RAM. Hive, invece, è progettato per batch: scalabile, distribuito, resistente a fallimenti e capace di sfruttare il cluster intero per throughput massimo.

- In sintesi: 
a) usa **Hive per trasformazioni pesanti e ripetute (ETL/batch)**, 
b) usa **Impala per letture puntuali e veloci (BI/analisi)**.

Punto chiave:
Hive e' la "casa dei dati" e un motore SQL batch (MapReduce/Tez/Spark): interroga ma con latenza di secondi/minuti.
Impala e' il "motore di interrogazione" low-latency MPP in-memory.

Esempio:
CREATE TABLE vendite (...);   -- Hive
SELECT * FROM vendite;        -- Impala


--------------------------------------------------


4) APACHE SQOOP

Cosa è:
Sqoop è uno strumento di data transfer, non un motore SQL.

Cosa fa:
- Importa dati da database relazionali verso Hadoop (HDFS/Hive)
- Esporta dati da Hadoop verso database relazionali (Sono quelli basati su tabelle, schema fisso, SQL e transazioni ACID: Esempi diffusi:

Oracle Database
Microsoft SQL Server
PostgreSQL
MySQL e MariaDB
IBM Db2
SQLite (embedded)
Varianti cloud compatibili: Amazon RDS/Aurora (MySQL/PostgreSQL/SQL Server/Oracle), Google Cloud SQL/AlloyDB (PostgreSQL), Azure SQL Database (SQL Server) etc.

- è basato su MapReduce

Quando usarlo:
- Ingestione dati da DB a Hadoop
- Estrazione dati da Hadoop verso DB
- Non per query o analisi


--------------------------------------------------


5) CHI GESTISCE I CONTENUTI DELLE RIGHE DELLE TABELLE

Le righe delle tabelle:
- NON stanno in Hive
- NON stanno in Impala
- Stanno nei file sullo storage

Storage (HDFS / S3 / ADLS):
- Gestisce i file fisici che contengono le righe
- Garantisce durabilità , replica e permessi

Hive Metastore:
- Gestisce i metadati (schema, tabelle, partizioni, path)

Motori SQL (Hive / Impala):
- Leggono e scrivono creando nuovi file
- Non aggiornano singole righe in modo transazionale


--------------------------------------------------


6) DATI E METADATI

Dati:
- Contenuto informativo vero e proprio
- Valori e righe delle tabelle
- Memorizzati nei file (Parquet, ORC, CSV)

Metadati:
- Informazioni che descrivono i dati
- Schema, colonne, tipi, partizioni, percorsi


Metadati tecnici:
- Struttura e aspetti IT
- Tipi di dato, formati, partizioni, statistiche

Metadati di business:
- Significato del dato
- Definizioni KPI, descrizioni campi
- Data owner, regole di qualitÃ , GDPR


--------------------------------------------------


7) COSA SI TROVA IN SQL

Precisazione: **SQL è un linguaggio di query, non un contenitore di dati**. I dati risiedono nel **database** sottostante; SQL è il motore che li interroga.

Con SQL (eseguito su un database) trovi:

**I dati** (memorizzati nel database, interrogabili via SQL)
- Contenuto delle tabelle (righe e colonne) accessibili con SELECT
- Esempio: `SELECT nome, importo FROM vendite WHERE anno = 2024;` interroga il database e restituisce le righe
- Nota: i dati stanno nel database (HDFS, filesystem, storage relazionale, ecc.); SQL li legge

**I metadati tecnici**
- Informazioni su schema, tabelle, colonne, tipi, indici, vincoli
- Memorizzati in cataloghi di sistema (es. `information_schema` in SQL standard, `sys.*` in SQL Server, `pg_*` in PostgreSQL)
- Esempio: `SELECT * FROM information_schema.tables;` mostra tutte le tabelle del database
- Includono: nomi colonne, tipi di dato, dimensioni, statistiche, chiavi primarie/esterne

In genere NON trovi in SQL:

**Metadati di business**
- Significato dei dati (es. "questo campo rappresenta il reddito lordo annuale")
- Proprietari dei dati (data owner)
- Definizioni KPI, glossario aziendale
- Regole di validazione personalizzate
- Catalogazione semantica

**Governance e regole di qualità**
- Policy di sicurezza e accesso
- Regole di conformità GDPR, audit trail
- Metriche di qualità e anomalie
- Ciclo di vita dei dati (retention, archivio)
- Lineage (provenienza e trasformazioni dei dati)

Questi si trovano in strumenti specifici: **data catalog** (es. Alation, Collibra), **metadata repository** (es. Apache Atlas), **data governance platform**


--------------------------------------------------


8) DISEGNO LOGICO A STRATI

UTENTI / BI
(report, dashboard, SQL)
        â–²
        |
MOTORI SQL
(Impala, Hive, Spark SQL)
        â–²
        |
METADATI
(Hive Metastore)
        â–²
        |
STORAGE
(HDFS / S3 / ADLS)
â†’ QUI STANNO LE RIGHE


--------------------------------------------------


9) CONFRONTO CON DATABASE RELAZIONALI CLASSICI

**Database relazionali classici (Oracle, PostgreSQL, MySQL):**

*Architettura integrata (Storage + SQL + Transazioni = unico sistema)*
- Storage fisico (tabelle, indici), motore SQL e gestore di transazioni sono strettamente integrati e comunicano continuamente
- Il database sa sempre dove sta ogni riga e può localizzarla in millisecondi
- Esempio: Oracle memorizza le righe di una tabella, gli indici, e il log delle transazioni nello stesso database, controllato da un unico motore

*UPDATE e DELETE a livello riga*
- Puoi localizzare una riga specifica (es. `UPDATE vendite SET importo = 100 WHERE id = 5;`)
- Il database trova la riga, la modifica in-place e aggiorna gli indici
- Se il server crasha durante l'UPDATE, il log di transazione consente il rollback

*Transazioni ACID*
- **Atomicity**: l'UPDATE intera o non accade mai (no stati parziali)
- **Consistency**: il database rimane in uno stato valido
- **Isolation**: UPDATE di utenti diversi non si interferiscono
- **Durability**: una volta committed, i dati sono persistiti e sicuri anche se il server crasha
- Esempio: `BEGIN; UPDATE conto SET saldo = saldo - 100 WHERE id = 1; UPDATE conto SET saldo = saldo + 100 WHERE id = 2; COMMIT;` garantisce che o entrambi gli UPDATE succedon o nessuno

**Hadoop / Hive / Impala:**

*Architettura separata (Storage vs Motori SQL)*
- Storage (HDFS/S3) contiene i file; il metastore descrive come leggerli; i motori SQL leggono/scrivono creando nuovi file
- Non esiste un'integrazione stretta: non puoi localizzare una singola riga rapidamente
- Esempio: per modificare una riga in un file Parquet su HDFS, devi leggere l'intero file, modificare la riga in memoria, e riscrivere il file completo (oppure usare formati transazionali come Delta/Iceberg)

*UPDATE e DELETE implicano riscrivere file interi*
- `UPDATE vendite SET importo = 100 WHERE id = 5;` non localizza la riga: Hive legge il blocco (o il file intero), modifica in memoria, riscrive
- Per grandi file (GB/TB), riscrivere l'intero file è costoso e lento
- Per questo Hadoop è pensato per INSERT massivi o riscritture complete, non aggiornamenti puntuali
- Nota: Delta Lake e Iceberg aggiungono log transazionali e snapshot, migliorando UPDATE/DELETE, ma il concetto rimane: dati su storage distribuito

*ACID non nativo*
- Hadoop non garantisce natively ACID per UPDATE/DELETE riga-per-riga
- Se due utenti scrivono contemporaneamente, Hadoop non serializza: risultato imprevedibile
- Se Hive crasha durante una scrittura, i dati parziali rimangono nel file
- Soluzione moderna: **Delta Lake**, **Iceberg**, **Hudi** aggiungono log di transazioni e snapshot per simulare ACID
- Esempio: Delta Lake su Hadoop può fare `UPDATE tabella SET colonna = valore WHERE condizione;` in modo ACID, usando log transazionali

**Confronto rapido:**

| Aspetto                  | Database relazionali      | Hadoop/Hive/Impala                  |
|--------------------------|---------------------------|----------------------------------   |
| Architettura             | Integrata (1 sistema)     | Separata (storage + motori)         |
| Localizzazione riga      | Veloce (indici, pointer)  | Lenta (leggi file/blocco)           |
| UPDATE riga singola      | Veloce, ACID              | Lento, riscrive file/blocco         |
| Scalabilità              | Verticale (server potente)| Orizzontale (molti nodi)            | 
| Volume dati tipico       | Gigabyte/Terabyte         | Terabyte/Petabyte                   |
| Caso d'uso               | Transazioni, BI operazionale | ETL batch, big data analytics    |
| ACID nativo              | Sì                        | No (ma Delta/Iceberg lo aggiungono) |

**Quando usare Database relazionali (MySQL, PostgreSQL, Oracle):**

- **Transazioni online** (OLTP - Online Transaction Processing): applicazioni bancarie, e-commerce, sistemi di ordini dove servono UPDATE/DELETE puntuali e ACID
- **Dati con aggiornamenti frequenti**: CRM, ERP, sistemi amministrativi dove le righe cambiano continuamente
- **Basse latenze critiche**: applicazioni web, mobile, real-time che richiedono risposta in millisecondi
- **Volume dati moderato**: centinaia di gigabyte/pochi terabyte (scalabili verticalmente con server potenti)
- **Concorrenza alta**: molti utenti che leggono/scrivono contemporaneamente su stesse righe
- **Integrità garantita**: dati finanziari, medici, legali dove gli errori costano molto

Esempi pratici:
- Banca: quando un cliente preleva 100€, aggiornare il saldo deve essere atomico (ACID)
- E-commerce: gestire inventario, ordini, pagamenti in tempo reale
- Sistema CRM: aggiornare contatti, note, follow-up continuamente

**Quando usare Hadoop/Hive/Impala:**

- **Big Data Analytics** (OLAP - Online Analytical Processing): analisi storiche, BI, data science su enormi volumi
- **ETL batch notturni**: processare gigabyte/terabyte di dati una volta al giorno, senza necessità di aggiornamenti puntuali
- **Dati inseriti una volta e letti molte volte**: log, sensori IoT, events che non cambiano
- **Scalabilità orizzontale necessaria**: dati che non entrano in un server, servono cluster di 100+ nodi
- **Latenza accettabile**: risposte in secondi/minuti vanno bene (non millisecondi)
- **Varietà di dati**: CSV, JSON, immagini, log, file non strutturati

Esempi pratici:
- Analista dati: analizzare 1 TB di transazioni del 2024 per trovare trend di vendita (una query, non UPDATE)
- Data lake aziendale: raccogliere log da 1000 server e analizzarli con Hive
- Machine learning: leggere terabyte di dati storici per addestrare modelli
- Reporting notturno: ogni notte ricalcolare report aggregati su milioni di righe

**Ibrido (Conviene a volte?)**

Alcune aziende usano **entrambi**:
- **Database relazionale**: sistema operativo in tempo reale (e-commerce, CRM, transazioni)
- **Hadoop**: data warehouse e analytics su copia storica dei dati (estratti via Sqoop di notte)
- Esempio: MySQL per il sito e-commerce (aggiornamenti continui), Hive per analytics su vendite storiche (una volta al giorno)

**CASO REALE: AMAZON**

Amazon è l'esempio perfetto di architettura ibrida che usa entrambi i sistemi:

**MySQL/Aurora (database relazionale) per transazioni in tempo reale:**
- **Carrello acquisti**: quando aggiungi un prodotto, UPDATE immediato del carrello (ACID)
- **Ordini**: quando compri, INSERT dell'ordine e UPDATE dello stock (transazione atomica)
- **Pagamenti**: addebito carta, aggiornamento saldo, conferma ordine (tutto ACID o rollback)
- **Gestione account**: login, password, indirizzi, preferenze (UPDATE puntuali e veloci)
- **Inventario in tempo reale**: decremento quantità disponibile quando qualcuno ordina
- Latenza: millisecondi (se non è veloce, il cliente abbandona)
- Volume per singolo database: gigabyte/terabyte
- Motivo: servono transazioni ACID, UPDATE/DELETE riga-per-riga, concorrenza alta

**Hadoop/EMR/S3 (data lake e analytics) per big data:**
- **Raccomandazioni prodotti** ("Chi ha comprato questo ha comprato anche..."): analisi di miliardi di transazioni storiche con algoritmi ML
- **Analytics e BI**: report su vendite, trend, performance categorie, previsioni inventario
- **Click-stream analysis**: analizzare miliardi di click, pageview, navigazioni per ottimizzare il sito
- **Data lake**: raccogliere log da milioni di server, sensori, dispositivi IoT (Alexa, Kindle, etc.)
- **Machine learning**: addestrare modelli su petabyte di dati (classificazione immagini, NLP, fraud detection)
- **Data science**: esperimenti A/B, test pricing, segmentazione clienti
- Latenza: secondi/minuti/ore (batch notturni, report giornalieri)
- Volume: petabyte distribuiti su migliaia di nodi
- Motivo: volumi enormi, elaborazioni batch complesse, non servono UPDATE puntuali

**Flusso tipico:**
1. Cliente ordina su Amazon → **MySQL/Aurora** registra transazione (tempo reale, ACID)
2. Di notte, **Sqoop o AWS DMS** esportano ordini da MySQL a **S3/Hadoop**
3. **EMR (Hadoop/Spark)** elabora milioni di ordini e calcola raccomandazioni/analytics
4. Risultati salvati in **Redshift** (data warehouse) o **S3** (data lake)
5. Dashboard BI, algoritmi ML, data scientist leggono da Redshift/S3

**In sintesi:**
- **MySQL = sistema operativo** (transazioni live, bassa latenza, ACID)
- **Hadoop/S3/EMR = sistema analitico** (big data, ML, BI su dati storici)


--------------------------------------------------


10) PERCHè HADOOP NON è ACID DI DEFAULT

Hadoop nasce per:
- grandi volumi di dati
- elaborazioni batch: si intende eseguire lavorazioni su grandi volumi di dati in lotti programmati (spesso notturni), con pipeline composte da più fasi sequenziali. Sono ottimizzate per throughput e scalabilità, non per risposte immediate: tempi di avvio non istantanei, latenza in secondi/minuti, ma capacità di processare molti dati in modo robusto.
- scalabilitÃ  orizzontale

Non nasce per:
- transazioni
- concorrenza su singole righe
- update riga-per-riga

Motivo:
- i dati sono memorizzati in file grandi e distribuiti
- modificare una riga richiede riscrivere file

Soluzione moderna:
- utilizzo di formati di tabella come Iceberg, Hudi o Delta Lake
- gestione di snapshot, versioni e storico

