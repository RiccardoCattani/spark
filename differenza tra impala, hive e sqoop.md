# DIFFERENZA TRA HIVE, IMPALA E SQOOP
(E TRA DATA WAREHOUSE E MOTORE SQL)

## CONCETTO FONDAMENTALE: GOVERNA VS POSSIEDE

**Storage** (HDFS, S3, ADLS)
- Possiede fisicamente i dati (file che contengono le righe effettive)
- Garantisce durabilità, replica e accesso distribuito
- Non conosce schemi, tabelle, strutture: vede solo byte e blocchi

**Data Warehouse** (Hive, Snowflake, BigQuery, Redshift)
- Governa ma non possiede i dati
- Organizza metadati, schemi, tabelle, partizioni, sicurezza e processi ETL
- Cataloga dove stanno i dati e come interpretarli
- I file fisici rimangono nello storage sottostante
- Nota: alcuni DW cloud (Snowflake, BigQuery) integrano anche lo storage, ma il principio resta

**Motori SQL** (Impala, Hive, Spark SQL, Trino)
- Leggono dati organizzati dal data warehouse
- Eseguono query e scrivono nuovi file
- Non aggiornano righe singole in modo transazionale (a meno di formati transazionali come Delta/Iceberg)

---

## DATA LAKE VS DATA WAREHOUSE

| Aspetto                | Data Lake                      | Data Warehouse              |
|------------------------|--------------------------------|-----------------------------|
| Possiede/Governa       | Possiede i dati fisicamente    | Governa (non possiede)      |
| Tipo dati              | Grezzi, strutturati e misti    | Strutturati                 |
| Schema                 | On-read (al momento della query) | On-write (alla scrittura) |
| Governance             | Limitata                       | Elevata                     |
| Utenti                 | Data scientist, ingegneri dati | Analisti, BI, business      |
| Esempi                 | Hadoop, S3, ADLS               | Hive, Snowflake, BigQuery   |

---

## DATI, METADATI E GOVERNANCE

**Dati (Righe)**
- Contenuto informativo vero: righe e colonne delle tabelle
- Memorizzati nei file (Parquet, ORC, CSV, ecc.) su storage
- Vivono in: HDFS, S3, ADLS (mai in Hive Metastore né in Impala)

**Metadati**
- Informazioni che descrivono i dati
- Schema, colonne, tipi, partizioni, percorsi, permessi, statistiche
- Gestiti da: Hive Metastore (per Hive/Impala)

**Governance del Data Warehouse**
- Schemi e tabelle
- Sicurezza: chi accede a quali dati
- Storico: versioni e modifiche nel tempo
- Processi ETL: estrazione, trasformazione e caricamento

**Cosa SI trova in SQL** (tramite un database/warehouse)
- I dati (interrogabili con SELECT)
- Metadati tecnici (schema, tipi, indici tramite information_schema, ecc.)

**Cosa NON si trova in SQL** (serve un data catalog)
- Metadati di business (significato, KPI, data owner)
- Governance: policy di sicurezza, GDPR, lineage, qualità dei dati
- Strumenti: Alation, Collibra, Apache Atlas

---

## MODELLI DI ESECUZIONE: BATCH VS INTERATTIVO

**Esecuzione Batch (Hive)**
- Ogni query scatena più fasi (map/shuffle/reduce) con startup di container YARN
- Overhead di avvio: secondi/decine di secondi per fase
- Dati letti da HDFS in blocchi, spesso riscritti su disco tra fasi (spill/shuffle)
- Fault tolerance: task falliti si riavviano automaticamente
- Latenza: secondi/minuti su grandi volumi
- Throughput: massimo, parallelizzazione su molti nodi
- Ideale per: ETL pesanti, elaborazioni notturne, grandi join/aggregazioni con shuffle massivi

**Esecuzione Interattiva (Impala)**
- In-memory, no MapReduce, motore MPP (Massively Parallel Processing)
- Long-running: i daemon Impala rimangono accesi, nessun startup YARN per query
- Latenza: secondi o millisecondi
- Limits: dati devono stare in RAM totale del cluster
- Non robusto per: job lunghi, riscritture massive, fault recovery
- Ideale per: BI, dashboard, analisi esplorativa, query puntuali

**Confronto rapido**
| Aspetto           | Hive (Batch)              | Impala (Interattivo)         |
|-------------------|---------------------------|------------------------------|
| Esecuzione        | MapReduce/Tez/Spark       | In-memory MPP                |
| Latenza           | Secondi/minuti            | Secondi/millisecondi         |
| Startup YARN      | Sì, overhead               | No, daemon long-running      |
| Throughput        | Massimo, altamente scalabile | Minore, limitato da RAM    |
| Fault tolerance   | Ottima (retry task)       | Scarsa (fine query)          |
| Adatto per        | ETL, batch notturni       | BI, analisi interattiva      |
| Non adatto per    | Query real-time / BI rapido | ETL pesanti, volumi enormi |

**One-liner**: Usa **Hive per trasformazioni pesanti**, **Impala per letture veloci**.

---

## APACHE HIVE

**Cos'è**: Data Warehouse + SQL-on-Hadoop

**Cosa fa**
- Definisce e governa tabelle, schemi, partizioni
- Gestisce metadati via Hive Metastore
- Traduce query HiveQL in MapReduce, Tez o Spark
- Interroga HDFS, S3, ADLS

**Quando usarlo**
- ETL complessi con molte fasi: join, aggregazioni, dedupliche, spill su disco
- Analisi su grandi volumi di dati (terabyte+)
- Batch notturni programmati
- Quando serve scalabilità orizzontale e fault tolerance
- NON per interrogazioni real-time (latenza troppo alta)

---

## APACHE IMPALA

**Cos'è**: Motore SQL MPP in-memory

**Cosa fa**
- Interroga gli stessi dati di Hive (su HDFS/S3)
- Usa gli stessi metadati (Hive Metastore)
- Fornisce SQL interattivo con bassa latenza

**Quando usarlo**
- BI, dashboard, report
- Analisi esplorativa e ad-hoc
- Query puntuali e veloci
- NON per ETL pesanti (limiti di memoria, nessuna fault tolerance)
- NON per batch notturni lunghi (architettura in-memoria, richiede query snelle)

---

## APACHE SQOOP

**Cos'è**: Strumento di data transfer (non motore SQL)

**Cosa fa**
- Importa dati da database relazionali (Oracle, MySQL, PostgreSQL, SQL Server, ecc.) verso Hadoop (HDFS/Hive)
- Esporta dati da Hadoop verso database relazionali
- Basato su MapReduce

**Quando usarlo**
- Ingestione iniziale da DB a Hadoop
- Estrazione da Hadoop verso DB
- NON per query o analisi

---

## TRANSAZIONI E ACID

**Database Relazionali Classici** (Oracle, MySQL, PostgreSQL)
- Architettura integrata: storage + motore SQL + gestore transazioni in un sistema unico
- UPDATE/DELETE puntuali: localizzazione veloce di singole righe via indici
- **ACID garantito**:
  - **A**tomicity: update intera o non accade
  - **C**onsistency: database rimane in stato valido
  - **I**solation: operazioni concorrenti non interferiscono
  - **D**urability: dati committed sono persistenti anche se crash
- Scalabilità: verticale (server potente)
- Caso d'uso: transazioni online (OLTP), e-commerce, CRM

**Hadoop/Hive/Impala**
- Architettura separata: storage (HDFS/S3) + motori SQL
- UPDATE/DELETE implicano riscrivere file interi (molto lento per volumi grandi)
- **ACID non nativo**: due scritture concorrenti = risultato imprevedibile; crash durante scrittura = dati parziali rimangono
- Scalabilità: orizzontale (molti nodi a basso costo)
- Caso d'uso: big data analytics (OLAP), ETL batch, machine learning
- Soluzione moderna: **Delta Lake**, **Iceberg**, **Hudi** aggiungono log transazionali e snapshot per ACID su Hadoop

**Quando scegliere**
- **Relazionali**: transazioni critiche, UPDATE/DELETE frequenti, latenza bassa, volume moderato (GB/TB)
  - Esempi: banca (prelievi atomici), e-commerce (ordini), CRM (contatti aggiornati)
- **Hadoop**: big data, elaborazioni batch, dati immutabili (insert-once), latenza accettabile, volume enorme (TB/PB)
  - Esempi: analytics, ML su terabyte di dati, log analysis, reporting notturni

---

## DISEGNO LOGICO A STRATI

```
┌─────────────────────────┐
│   UTENTI / BI           │
│ (report, dashboard, SQL)│
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│    MOTORI SQL           │
│ (Impala, Hive, Spark)   │
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│     METADATI            │
│   (Hive Metastore)      │
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│      STORAGE            │
│  (HDFS / S3 / ADLS)     │
│  ▶ QUI STANNO LE RIGHE  │
└─────────────────────────┘
```

---

## CASO REALE: AMAZON (ARCHITETTURA IBRIDA)

Amazon usa entrambi i sistemi:

**MySQL/Aurora** (DB relazionale - OLTP)
- Carrello acquisti: UPDATE immediato (ACID)
- Ordini: INSERT + UPDATE stock (transazione atomica)
- Pagamenti: atomicità critica (tutto o nulla)
- Inventario real-time: decremento immediato
- Latenza: millisecondi

**Hadoop/EMR/S3** (Data lake/warehouse - OLAP)
- Raccomandazioni: miliardi di transazioni storiche analizzate
- Analytics e BI: report su vendite, trend, previsioni
- Click-stream analysis: miliardi di click e navigazioni
- Machine learning: terabyte di dati per addestrare modelli
- Data science: A/B test, segmentazione, pricing
- Latenza: secondi/minuti/ore

**Flusso**:
1. Cliente ordina → **MySQL** registra (real-time, ACID)
2. Di notte → **Sqoop** esporta ordini a **S3**
3. **EMR** elabora milioni di ordini
4. Risultati in **Redshift** (DW) o **S3** (data lake)
5. Analytics, ML e BI leggono da Redshift/S3

**Sintesi**: MySQL = sistema operativo (transazioni live); Hadoop = sistema analitico (big data, ML, BI su dati storici).

---

## RIEPILOGO FINALE

| Componente    | Ruolo                                | Possiede/Governa Dati | Latenza           |
|---------------|--------------------------------------|----------------------|-------------------|
| **Storage**   | Memorizza file fisici                | Possiede              | N/A               |
| **Metastore** | Catalogo e governance                | Governa metadati      | N/A               |
| **Hive**      | DW + query batch                     | Governa (non possiede) | Secondi/minuti   |
| **Impala**    | Query interattivo MPP in-memory      | Governa (non possiede) | Secondi/milli    |
| **Sqoop**     | Data transfer DB ↔ Hadoop            | Trasporta (non governa) | Batch            |

**Governance vs Possesso**:
- Lo **storage possiede** fisicamente i dati (file su HDFS/S3/ADLS)
- Il **data warehouse governa** i dati (metadati, schemi, tabelle, sicurezza, processi)
- I **motori SQL leggono/scrivono** i dati secondo il governo del warehouse

