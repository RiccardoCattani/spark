# DIFFERENZA TRA HIVE, IMPALA E SQOOP
(E TRA DATA WAREHOUSE E MOTORE SQL)


1) DATA WAREHOUSE VS MOTORE SQL

Data Warehouse
- Governa e organizza i dati analitici
a) Definisce schemi, tabelle, partizioni e storico
b) Gestisce metadati, sicurezza e processi batch / ETL
- Può utilizzare uno o più motori SQL:
Tra i quali: Hive, Snowflake, BigQuery

Motore SQL
- Esegue query SQL
- Calcola e restituisce risultati
- Non governa i dati
- Dipende da uno storage o da un Data Warehouse, come tra gli altri:
Tra i quali Impala, Spark SQL, Trino

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
