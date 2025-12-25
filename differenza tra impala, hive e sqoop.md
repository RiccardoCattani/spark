# DIFFERENZA TRA HIVE, IMPALA E SQOOP
(E TRA DATA WAREHOUSE E MOTORE SQL)

## DATA LAKE VS DATA WAREHOUSE

**Data Lake**  (Es.Hadoop, Amazon S3, Azure Data Lake Storage)
- Brevemente: Possiede fisicamente i dati (Grezzi, strutturati e non strutturati)
- Un data lake è un sistema di archiviazione che raccoglie grandi quantità di dati grezzi, strutturati e non strutturati, provenienti da fonti diverse.
- I dati vengono memorizzati così come sono (schema-on-read), senza una struttura predefinita.
- È pensato per la scalabilità, la flessibilità e l’analisi di dati eterogenei (log, immagini, file, dati IoT, ecc.).
- Tipicamente utilizza storage distribuito come HDFS, S3, ADLS.
- Esempi: Hadoop, Amazon S3, Azure Data Lake Storage

**Data Warehouse** (Es. Hive, Snowflake, BigQuery, Redshift)
- Brevemente: governa, organizza e cataloga i dati
- Un data warehouse è un sistema che organizza, struttura e governa i dati ma non li possiede
- I dati sono strutturati in tabelle e schemi (schema-on-write), con qualità e coerenza garantite.
- il data warehouse non possiede fisicamente i dati: governa, organizza e cataloga i dati, ma questi risiedono nello storage sottostante (come HDFS, S3, ADLS). Il data warehouse gestisce metadati, schemi, tabelle e processi ETL, mentre i file con le righe dei dati sono nello storage. Alcuni data warehouse cloud integrano anche lo storage, ma la logica resta: il data warehouse governa, lo storage possiede i dati.
- Ottimizzato per query analitiche, BI e reporting.
- Gestisce metadati, sicurezza, storico e processi ETL, n

el dettaglio:
a) Gestione dei metadati: Tiene traccia delle informazioni che descrivono i dati (schema delle tabelle, tipi di colonne, partizioni, permessi, ecc.), facilitando la comprensione, la ricerca e l’utilizzo dei dati stessi. Si ricorda che i dati sono le informazioni che vuoi analizzare o conservee (es. le righe di una tabella: nomi, numeri, date, transazioni, ecc.), mentre i metadati sono sono le informazioni che descrivono i dati stessi (es. schema della tabella, nomi e tipi delle colonne, partizioni, permessi, percorso dei file, definizioni dei campi).
b) Sicurezza: Permette di definire chi può accedere a quali dati, impostando permessi e ruoli per utenti e gruppi, garantendo la protezione delle informazioni sensibili.
c) Storico: Consente di mantenere versioni storiche dei dati, tracciare le modifiche e gestire lo storico delle tabelle, utile per audit, analisi temporali e ripristino.
Processi ETL (Extract, Transform, Load): Supporta e governa i processi di estrazione, trasformazione e caricamento dei dati, assicurando che i dati siano puliti, coerenti e pronti per l’analisi.

### Differenze principali

| Caratteristica         | Data Lake                        | Data Warehouse                  |
|------------------------|----------------------------------|---------------------------------|
| Tipo di dati           | Grezzi, strutturati e non        | Strutturati                     |
| Schema                 | On-read (al momento della query) | On-write (alla scrittura)       |
| Governance             | Limitata                         | Elevata                         |
| Performance query      | Variabile                        | Ottimizzata per analisi         |
| Costo                  | Basso (storage scalabile)        | Più alto (ottimizzato, gestito) |
| Utenti tipici          | Data scientist, ingegneri dati   | Analisti, business, BI          |
| Esempi                 | Hadoop, S3, ADLS                 | Hive, Snowflake, BigQuery       |

**In sintesi:**
Il data lake è un “lago” di dati grezzi e flessibili, il data warehouse è un “magazzino” di dati strutturati e governati per analisi.

1) DATA WAREHOUSE VS MOTORE SQL

Data Warehouse
- Governa (Ma non possiede) e organizza i dati analitici (Le righe, ossia i dati fisici, risiedono nei file su HDFS/S3/ADLS. Hive/Impala leggono e scrivono quei file, e il metastore tiene il catalogo (tabelle, schemi, partizioni, permessi) 
a) Definisce schemi, tabelle, partizioni e storico
b) Gestisce metadati, sicurezza e processi batch / ETL
- Può utilizzare uno o più motori SQL:
Tra i quali: Hive, Snowflake, BigQuery
Attenzione: Hive è sia un data warehouse che un motore SQL

**Differenza tra File System (es. HDFS) e Data Warehouse**
- Un file system come HDFS si occupa solo di memorizzare file e cartelle, senza struttura o regole sui dati.
- Un data warehouse, invece, organizza i dati in tabelle, schemi e partizioni, gestisce metadati, sicurezza, storico e processi di caricamento/analisi. Fornisce strumenti per interrogare e governare i dati.
Per essere precisi, nel mondo Hive/Impala: il “warehouse” governa (schemi, tabelle, partizioni, metadati, sicurezza) ma le righe vivono nei file su HDFS/S3/ADLS. Il catalogo dice dove sono i file e come interpretarli; i motori SQL leggono/scrivono file nuovi, non tengono le righe “dentro” il metastore.
Eccezione: alcuni data warehouse cloud (Snowflake, BigQuery) integrano anche lo storage fisico, ma la logica resta la stessa: catalogo/metadati + motore; i dati stanno comunque in uno storage sottostante.

**Schema rapido (cosa fa chi)**
- HDFS: memorizza i file che contengono le righe.
- Data Warehouse (Hive Metastore + layer SQL batch di Hive): definisce schemi, tabelle, partizioni, governa metadati, sicurezza e processi ETL/batch.
- Motore SQL (es. Impala): interroga i dati già memorizzati, restituisce risultati; non governa i metadati.
**Box rapido: tabelle vs metadati vs dati**
- Tabelle: entita logiche nel Hive Metastore.
- Metadati: schema, colonne, partizioni, permessi nel catalogo.
- Dati (righe): file fisici su HDFS/S3/ADLS.


**Significato letterale di “warehouse”**
- “Warehouse” in inglese significa “magazzino”. Un data warehouse è quindi un “magazzino di dati”, cioè un sistema che raccoglie, organizza e conserva grandi quantità di dati per analisi e reportistica.

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

Cos’è:
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

Quando usarlo:
- Elaborazioni pesanti
- ETL complessi
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
- Non per ETL complessi o batch notturni

Punto chiave:
Hive è la “casa dei dati”
Impala è il “motore di interrogazione”

Esempio:
CREATE TABLE vendite (...);   -- Hive
SELECT * FROM vendite;        -- Impala


--------------------------------------------------


4) APACHE SQOOP

Cos’è:
Sqoop è uno strumento di data transfer, non un motore SQL.

Cosa fa:
- Importa dati da database relazionali verso Hadoop (HDFS/Hive)
- Esporta dati da Hadoop verso database relazionali
- È basato su MapReduce

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
- Garantisce durabilità, replica e permessi

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
- Data owner, regole di qualità, GDPR


--------------------------------------------------


7) COSA SI TROVA IN SQL

In SQL trovi:
- I dati (SELECT dalle tabelle)
- I metadati tecnici (cataloghi di sistema, information_schema)

In genere NON trovi:
- Metadati di business
- Governance e regole di qualità


--------------------------------------------------


8) DISEGNO LOGICO A STRATI

UTENTI / BI
(report, dashboard, SQL)
        ▲
        |
MOTORI SQL
(Impala, Hive, Spark SQL)
        ▲
        |
METADATI
(Hive Metastore)
        ▲
        |
STORAGE
(HDFS / S3 / ADLS)
→ QUI STANNO LE RIGHE


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


10) PERCHÉ HADOOP NON È ACID DI DEFAULT

Hadoop nasce per:
- grandi volumi di dati
- elaborazioni batch
- scalabilità orizzontale

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

