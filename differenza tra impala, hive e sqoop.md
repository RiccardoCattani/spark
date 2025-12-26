# DIFFERENZA TRA HIVE, IMPALA E SQOOP
(E TRA DATA WAREHOUSE E MOTORE SQL)

## DATA LAKE VS DATA WAREHOUSE

**Data Lake**  (Es.Hadoop, Amazon S3, Azure Data Lake Storage)
- Brevemente: Possiede fisicamente i dati (Grezzi, strutturati e non strutturati)
- Un data lake Ã¨ un sistema di archiviazione che raccoglie grandi quantitÃ  di dati grezzi, strutturati e non strutturati, provenienti da fonti diverse.
- I dati vengono memorizzati cosÃ¬ come sono (schema-on-read), senza una struttura predefinita.
- Ãˆ pensato per la scalabilitÃ , la flessibilitÃ  e lâ€™analisi di dati eterogenei (log, immagini, file, dati IoT, ecc.).
- Tipicamente utilizza storage distribuito come HDFS, S3, ADLS.
- Esempi: Hadoop, Amazon S3, Azure Data Lake Storage

**Data Warehouse** (Es. Hive, Snowflake, BigQuery, Redshift)
- Brevemente: governa, organizza e cataloga i dati
- Un data warehouse Ã¨ un sistema che organizza, struttura e governa i dati ma non li possiede
- I dati sono strutturati in tabelle e schemi (schema-on-write), con qualitÃ  e coerenza garantite.
- il data warehouse non possiede fisicamente i dati: governa, organizza e cataloga i dati, ma questi risiedono nello storage sottostante (come HDFS, S3, ADLS). Il data warehouse gestisce metadati, schemi, tabelle e processi ETL, mentre i file con le righe dei dati sono nello storage. Alcuni data warehouse cloud integrano anche lo storage, ma la logica resta: il data warehouse governa, lo storage possiede i dati.
- Ottimizzato per query analitiche, BI e reporting.
- Gestisce metadati, sicurezza, storico e processi ETL
Nel dettaglio:
a) Gestione dei metadati: Tiene traccia delle informazioni che descrivono i dati (schema delle tabelle, tipi di colonne, partizioni, permessi, ecc.), facilitando la comprensione, la ricerca e lâ€™utilizzo dei dati stessi. Si ricorda che i dati sono le informazioni che vuoi analizzare o conservee (es. le righe di una tabella: nomi, numeri, date, transazioni, ecc.), mentre i metadati sono sono le informazioni che descrivono i dati stessi (es. schema della tabella, nomi e tipi delle colonne, partizioni, permessi, percorso dei file, definizioni dei campi).
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

Data Warehouse
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

Motore SQL
- Esegue query SQL
- Calcola e restituisce risultati
- Non governa i dati
- Dipende da uno storage o da un Data Warehouse, come tra gli altri:
Impala, Spark SQL, Trino

One-liner:
Il Data Warehouse governa i dati, il motore SQL li interroga.

--------------------------------------------------


2) APACHE HIVE

Cosa è ?
Hive è un Data Warehouse SQL-on-Hadoop.

Cosa fa:
- Definisce e governa tabelle, schemi e partizioni
- Gestisce i metadati tramite Hive Metastore
- Permette di interrogare dati su HDFS e object storage (S3, ADLS)
- Usa HiveQL (simile a SQL)
- Traduce le query in MapReduce, Tez o Spark

Caratteristiche:
- Orientato a elaborazioni batch
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

Cosa è:
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
- Non per ETL complessi o batch notturni

Punto chiave:
Hive e' la "casa dei dati" e un motore SQL batch (MapReduce/Tez/Spark): interroga ma con latenza di secondi/minuti.
Impala e' il "motore di interrogazione" low-latency MPP in-memory.

Esempio:
CREATE TABLE vendite (...);   -- Hive
SELECT * FROM vendite;        -- Impala


--------------------------------------------------


4) APACHE SQOOP

Cosâ€™Ã¨:
Sqoop Ã¨ uno strumento di data transfer, non un motore SQL.

Cosa fa:
- Importa dati da database relazionali verso Hadoop (HDFS/Hive)
- Esporta dati da Hadoop verso database relazionali
- Ãˆ basato su MapReduce

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
- Garantisce durabilitÃ , replica e permessi

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

In SQL trovi:
- I dati (SELECT dalle tabelle)
- I metadati tecnici (cataloghi di sistema, information_schema)

In genere NON trovi:
- Metadati di business
- Governance e regole di qualitÃ 


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

Database relazionali (Oracle, PostgreSQL, MySQL):
- Storage, SQL e gestione delle righe sono un unico sistema
- Supportano UPDATE e DELETE a livello riga
- Garantiscono transazioni ACID

Hadoop / Hive / Impala:
- Storage separato dai motori SQL
- Aggiornare una riga implica riscrivere file
- ACID non nativo


--------------------------------------------------


10) PERCHÃ‰ HADOOP NON Ãˆ ACID DI DEFAULT

Hadoop nasce per:
- grandi volumi di dati
- elaborazioni batch
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

