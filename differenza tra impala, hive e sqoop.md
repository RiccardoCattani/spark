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
| Funzione principale    | Storage (memorizza)            | Query + Governance          |
| Esegue query SQL       | No (solo storage)              | Sì (SELECT, JOIN, ecc.)     |
| Tipo dati              | Grezzi, strutturati e misti    | Strutturati                 |
| Schema                 | On-read (al momento della query) | On-write (alla scrittura) |
| Governance tecnica     | Limitata o assente             | Elevata (schemi, permessi, ETL) |
| Governance business    | Richiede Data Catalog          | Richiede Data Catalog       |
| Utenti                 | Data scientist, ingegneri dati | Analisti, BI, business      |
| Esempi                 | Hadoop, S3, ADLS               | Hive, Snowflake, BigQuery   |

**⚠️ Attenzione: Data Lake ≠ Data Catalog**

| Aspetto              | Data Lake                                    | Data Catalog                                   |
|----------------------|----------------------------------------------|------------------------------------------------|
| **Cosa è**           | Storage system (memorizza dati fisicamente)   | Metadata/Governance platform (documenta dati)  |
| **Possiede/Governa** | Possiede (memorizza file)                    | Governa (non possiede)                         |
| **Contiene**         | Dati grezzi, strutturati e misti             | Definizioni business, ownership, policy, lineage |
| **Esempi**           | Hadoop/HDFS, S3, ADLS                        | Alation, Collibra, Apache Atlas                |
| **Funzione**         | Memorizzare dati durevolmente                | Catalogare dove stanno i dati e cosa significano |
| **Conosce schemi**   | No (on-read)                                 | Sì (documenta schemi e strutture)              |
| **Esegue query**     | No (solo storage)                            | No (cataloga, non esegue query)                |

**Rapporto pratico:**
- **Data Lake** = magazzino fisico (storage, possiede i dati)
- **Data Warehouse** = responsabile della lettura e della struttura (governa tabelle, schemi, permessi SQL)
- **Data Catalog** = bibliotecario (sa cosa c'è e cosa significa, non memorizza dati)

**⚠️ Data Catalog NON è un contenitore**

Il Data Catalog **non memorizza dati**, non è un contenitore:
- **Non contiene** dati effettivi (righe, file, tabelle)
- **Non contiene** metadati tecnici (se non come riferimenti/indici)
- **Documenta e cataloga** i dati/tabelle che vivono in altri sistemi (Data Warehouse, Storage, Database)
- **Punta a** e aggrega informazioni da altre piattaforme

**Esempio pratico:**
```
┌─────────────────────────────┐
│ Storage (S3, HDFS)          │
├─ file_vendite.parquet (1GB) │
└─────────────────────────────┘
           ↑ contiene dati
           │
┌─────────────────────────────┐
│ Data Warehouse (Hive)       │
├─ TABLE vendite (schema)     │
└─────────────────────────────┘
           ↑ governa tabelle
           │
┌─────────────────────────────┐
│ Data Catalog (Alation)      │
├─ "vendite = reddito lordo"  │
├─ "owner: Mario Rossi"       │
├─ "sensibile GDPR"           │
├─ "usata in 5 report"        │
└─ "lineage: Salesforce→Hive" │
└─────────────────────────────┘
     documenta e cataloga
     (NON memorizza dati)
```

È come la **Biblioteca Nazionale**: 
- Non contiene i libri fisici (come il magazzino di Amazon)
- Non esegue transazioni sui libri (come un sistema di gestione di biblioteca)
- Contiene catalogo, indici, e metadati su chi scrive libri, quando sono stati pubblicati, quale argomento trattano
- Punta ai libri che vivono in altre biblioteche

**⚠️ Data Catalog ≠ Data Warehouse**

| Aspetto              | Data Warehouse (Hive, Snowflake, BigQuery) | Data Catalog (Alation, Collibra, Atlas) |
|----------------------|--------------------------------------------|-----------------------------------------|
| **Esegue query SQL** | Sì: SELECT, INSERT, UPDATE, DELETE         | No: non esegue query                    |
| **Governa dati**     | Sì: schemi, tabelle, partizioni, permessi  | No: cataloga informazioni da altri sistemi |
| **Memorizza dati**   | No (governa, non possiede); dati in storage | No (non memorizza dati, solo metadati business) |
| **Controlla accesso**| Sì: RBAC/ABAC, masking, filtri riga/colonna | No: eventuali preview passano per il motore |
| **Metadati**         | Tecnici: information_schema, statistiche, indici | Business: significato, ownership, policy, lineage |
| **Utenti**           | Ingegneri, analisti, BI (query e trasformazioni) | Data steward, governance, business (ricerca significato) |
| **Esempio di query** | `SELECT * FROM vendite WHERE anno=2024` → righe reali | `search("vendite", filter="PII")` → metadati business |

**In una frase:**
- **Data Warehouse** = motore che esegue query e governa la struttura tecnica (schemi, permessi SQL)
- **Data Catalog** = libreria che documenta il significato business e la governance (non possiede né interroga dati)

**Dove vivono fisicamente i metadati?**

I metadati del Data Catalog si trovano **nel database interno del Catalog stesso**, non nei sistemi che cataloga:

```
┌─────────────────────────┐
│ Storage (S3, HDFS)      │
│ file_vendite.parquet    │
└─────────────────────────┘
           ↑
           
┌──────────────────────────────┐
│ Data Warehouse (Hive)        │
│ TABLE vendite (schema)       │
│ Hive Metastore (metadati)    │
└──────────────────────────────┘
           ↑
           
┌────────────────────────────────────────┐
│ Data Catalog (Alation/Collibra/Atlas)  │
│ ┌──────────────────────────────────┐   │
│ │ Database interno del Catalog:    │   │
│ │ - "vendite = reddito lordo"      │   │
│ │ - "owner: Mario Rossi"           │   │
│ │ - "sensibile GDPR"               │   │
│ │ - "usata in 5 report"            │   │
│ │ - "lineage: Salesforce→Hive"     │   │
│ └──────────────────────────────────┘   │
└────────────────────────────────────────┘
```

**Dove vivono fisicamente:**
- **Alation**: database interno (PostgreSQL, MySQL, Oracle) + storage per documenti/descrizioni
- **Collibra**: database interno (PostgreSQL) + metadata repository centralizzato
- **Apache Atlas**: HBase (per metadati) + Elasticsearch (per ricerca) nel cluster Hadoop

**Caratteristica importante: indipendenza**
- Se il Data Warehouse (Hive) cade, il Data Catalog rimane online (documenta comunque cosa c'era)
- Se il Data Catalog cade, i dati rimangono intatti nel Storage/DW (il Catalog non li controlla, solo li documenta)
- I metadati del Catalog sono **completamente separati** e indipendenti dall'architettura dati


## DATI, METADATI E GOVERNANCE

**Dati (Righe)**
- Contenuto informativo vero: righe 
- Memorizzati nei file (Parquet, ORC, CSV, ecc.) su storage
- Vivono in: HDFS, S3, ADLS (mai in Hive Metastore né in Impala)

**Metadati**
- Informazioni che descrivono i dati
- Schema, tabelle, colonne, tipi, partizioni, percorsi, permessi, statistiche
- Nota: la "tabella" come oggetto (nome, schema, proprietà, location, formati) è metadato; le righe contenute sono dati
- Gestiti da: Hive Metastore (per Hive/Impala)

### SQL vs Data Catalog (Riassunto veloce)

| Aspetto         | SQL (motore)                                               | Data Catalog (Alation/Collibra/Atlas)                          |
|-----------------|-------------------------------------------------------------|-----------------------------------------------------------------|
| Scopo           | Leggere/scrivere dati, eseguire query, gestire schema       | Documentare significato, ownership, policy, lineage             |
| Gestisce        | Tabelle, colonne, tipi, DDL/DML, permessi (GRANT/REVOKE)    | Definizioni business, KPI, classificazioni (PII), qualità, glossario |
| Enforcement     | Sì: controlli d’accesso, masking, filtri a livello riga/colonna | No: non esegue query; eventuali preview rispettano i permessi della sorgente |
| Preview dati    | Nativo (via query)                                          | Facoltativo; passa tramite la connessione al motore             |
| Dove vive       | Motore dati (Hive/Impala/Snowflake/PostgreSQL/…)            | Piattaforma separata                                            |
| Utenti          | Ingegneri, analisti, BI                                     | Data steward, governance, business                              |
| Esempi          | Hive, Impala, Spark SQL, Snowflake, BigQuery                | Alation, Collibra, Apache Atlas                                 |

In una frase: SQL governa struttura e accesso; il Catalog governa significato e regole.

**Gerarchia organizzativa: Database > Schema > Tabella**

```
DATABASE (contenitore generale)
│
├── SCHEMA (gruppo logico / namespace)
│   │
│   └── TABELLA (struttura dati con righe/colonne)
│       │
│       ├── COLONNA (campo: nome + tipo)
│       └── RIGA (record singolo)
```

**Schema (o Database in Hive)** = Contenitore/Namespace
- Raggruppa tabelle correlate logicamente
- Evita conflitti di nome (es. `finance.vendite` vs `marketing.vendite`)
- Gestisce permessi a livello di gruppo

**Tabella** = Struttura dati effettiva
- Contiene righe (record) e colonne (campi)
- Ha schema definito (nomi colonne + tipi di dato)

**Esempio pratico:**
```sql
-- Creare uno schema (in Hive si chiama DATABASE)
CREATE DATABASE finance;
CREATE DATABASE marketing;

-- Creare tabelle in schemi diversi
CREATE TABLE finance.vendite (id INT, importo DECIMAL, data DATE);
CREATE TABLE marketing.vendite (id INT, prodotto VARCHAR, campagna VARCHAR);
-- ↑ Stesso nome "vendite", ma tabelle diverse!

-- Accedere alle tabelle
SELECT * FROM finance.vendite;      -- Tabella del team Finance
SELECT * FROM marketing.vendite;    -- Tabella diversa del team Marketing
```

**Analogia rapida:**
```
Database = Biblioteca intera
Schema = Piano della biblioteca (es. Piano 1: Narrativa, Piano 2: Tecnica)
Tabella = Scaffale con libri (es. Romanzi, Poesia, Informatica)
```

**Governance del Data Warehouse** (governance TECNICA)

Il Data Warehouse gestisce la governance **tecnica**, ma non quella **di business**:

✅ **Governance TECNICA** (gestita dal Data Warehouse):
- Schemi, tabelle, partizioni e tipi di dato
- Sicurezza di accesso: permessi SQL (GRANT/REVOKE su tabelle)
- Storico tecnico: versioni, snapshot (con Delta/Iceberg)
- Processi ETL: orchestrazione di trasformazioni e caricamenti
- Metadati tecnici: formati (Parquet/ORC), paths, statistiche

❌ **Governance DI BUSINESS** (serve un Data Catalog esterno):
- Significato aziendale: cosa rappresenta ogni campo per l'azienda
- Ownership: chi è il data owner, chi è responsabile
- Policy aziendali: GDPR, retention, classificazione (pubblico/riservato)
- Lineage completo: provenienza e tutte le trasformazioni subite
- Qualità: anomalie, duplicati, completezza, validità
- Strumenti: Alation, Collibra, Apache Atlas

**Esempio pratico:**
```sql
-- Data Warehouse gestisce (governance tecnica):
CREATE TABLE vendite (id INT, importo DECIMAL, data DATE);
GRANT SELECT ON vendite TO utente_finance;
-- ✅ Sa: schema, permessi SQL

-- Data Catalog gestisce (governance business):
"vendite.importo = reddito lordo mensile (include bonus)"
"Owner: Mario Rossi (CFO), sensibile GDPR, retention 7 anni"
-- ✅ Sa: significato, policy, ownership
```

**Accesso a dati sensibili (GDPR): Alation vs SQL**
- **Alation** è un Data Catalog: non memorizza né elabora i dati; mostra metadati e, se configurato, può offrire un "data preview".
- Qualsiasi **preview** in Alation esegue query sulla sorgente tramite una connessione/account e **rispetta i permessi** del motore sottostante: non è un bypass dei controlli.
- L’accesso ai dati sensibili si **controlla nel motore** (Hive/Impala/DB) e nello storage: RBAC/ABAC, column-level masking, row-level filtering, encryption, auditing.
- In Hadoop (Hive/Impala) usare **Apache Ranger** per:
  - Column masking (es. mascherare `email` o offuscare parzialmente `codice_fiscale`)
  - Row filter (limitare righe visibili per reparto/paese)
  - Audit centralizzato delle query
- Alternative/varianti: **Sentry** (legacy), **Lake Formation** (AWS), **Unity Catalog** (Databricks) con tag/policy.
- Per GDPR (diritto all’oblio/retention): usare **Delta/Iceberg/Hudi** per `DELETE` e gestione snapshot/`VACUUM`.

**Esempi pratici: masking e filtri (GDPR)**
- **Ranger – Column Masking**: imposta una policy sulla colonna `email` di `finance.vendite` (azione SELECT) con mascheramento parziale (es. mostra solo i primi 3 caratteri e il dominio) o totale (NULL/hash). La maschera si applica a tutte le query, viste incluse.
- **Ranger – Row-level Filter**: definisci un filtro di riga, ad es. `cntry_cd = 'IT'` per il ruolo `FINANCE_IT`, oppure usa attributi utente (es. `${USER.country}`) per filtri dinamici per paese/reparto.
- **Ranger + Atlas (Tag-based)**: tagga in Atlas le colonne PII (es. `email`, `codice_fiscale`) e crea in Ranger una policy “per tag” che applica maschere/filtri automaticamente a tutte le tabelle con quel tag.

*Pattern SQL-only (viste sicure), se non hai Ranger:*
```sql
-- Mascheramento colonna email tramite vista (Hive/Impala)
CREATE SCHEMA IF NOT EXISTS secure;
CREATE OR REPLACE VIEW secure.vendite_masked AS
SELECT
  id,
  CONCAT(SUBSTR(email, 1, 3), '***', SUBSTR(email, LOCATE('@', email))) AS email_mascherata,
  data,
  cntry_cd
FROM finance.vendite;

-- Filtro per righe (versione statica)
CREATE OR REPLACE VIEW secure.vendite_it AS
SELECT * FROM finance.vendite WHERE cntry_cd = 'IT';

-- Filtro per righe basato sull'utente (mappa utente→paese)
-- Nota: in Hive usa CURRENT_USER(); in Impala la funzione è USER()
CREATE TABLE IF NOT EXISTS security.user_country (utente STRING, cntry_cd STRING);
CREATE OR REPLACE VIEW secure.vendite_per_utente AS
SELECT v.*
FROM finance.vendite v
JOIN security.user_country m
  ON m.utente = CURRENT_USER()
 AND v.cntry_cd = m.cntry_cd;
```

Suggerimenti operativi:
- Preferisci policy centralizzate (Ranger/Lake Formation/Unity Catalog) a logica applicativa nelle viste: sono auditate e coerenti.
- Abilita audit di query e usa cifratura a riposo/in transito (HDFS Transparent Encryption, S3/KMS, TLS).
- Per GDPR “right to be forgotten”, combina `DELETE` su formati transazionali (Delta/Iceberg/Hudi) con procedure di compattazione/`VACUUM` secondo le policy di retention.

**Cosa SI trova con SQL** (il database conosce)

*I dati effettivi*
```sql
SELECT nome, importo FROM vendite WHERE anno = 2024;
-- Restituisce: Ana | 150€, Marco | 200€, Sofia | 300€, ...
-- SQL accede ai dati fisici e li legge
```

*Metadati tecnici* (il database conosce la sua struttura)
```sql
-- Nome colonne e tipi (Hive/Impala)
DESCRIBE vendite;
-- Oppure più dettagliato:
DESCRIBE EXTENDED vendite;
-- Risultato: nome_colonna | tipo_dato | commento
-- importo (DECIMAL), data (DATE), nome (VARCHAR), ecc.

-- Elencare tutte le colonne con formato dettagliato
SHOW COLUMNS FROM vendite;
-- Risultato: column_name, data_type, comment

-- Struttura completa della tabella (DDL)
SHOW CREATE TABLE vendite;
-- Risultato: lo statement CREATE TABLE completo con location, formato, partizioni

-- Nota: information_schema esiste in DB relazionali (MySQL, PostgreSQL)
-- ma Hive/Impala usano DESCRIBE, SHOW COLUMNS, SHOW CREATE TABLE
```

**Cosa NON si trova con SQL** (serve un Data Catalog esterno)

*Metadati di business* (il significato aziendale dei dati)
- "La colonna `importo` rappresenta il **reddito lordo annuale** (comprende bonus e incentivi)"
- "Il KPI `conversion_rate` è calcolato come **(ordini / visitatori) * 100**"
- "La tabella `vendite` è **proprietà del team Finance** e curata da Mario Rossi (data owner)"
- "Questo dato è **sensibile: GDPR**, accesso limitato al solo team Finance e Compliance"

*Governance e compliance* (regole aziendali e tracciabilità)
- **Lineage**: "Da dove viene questo dato? Salesforce → ETL → Hadoop → report finale (con 3 trasformazioni)"
- **Qualità**: "Questo dato ha problemi? 5% di valori duplicati, 2% di null anomali, 1 outlier rilevato"
- **Audit trail**: "Chi ha modificato questa colonna? (storico completo di cambiamenti e responsabili)"
- **Retention policy**: "Quanto tempo conservo questi dati? Cancellare dopo 5 anni (GDPR 'right to be forgotten')"
- **Classificazione**: "Questo è pubblico, ristretto, sensibile o strettamente confidenziale?"

*Strumenti specializzati* (dove mettere questi metadati)
- **Alation**: Catalogo di business - documenta significato, ownership, KPI, criticità
- **Collibra**: Governance platform - policy complete, compliance, audit trail
- **Apache Atlas**: Metadata repository - lineage, dipendenze, trasformazioni (open-source)

**Perché SQL non può contenere questi dati?**
- SQL è disegnato per dati strutturati (tabelle, righe, colonne) e query transazionali
- I metadati di business sono semantica e regole: difficili da organizzare in tabelle SQL
- Cambian frequentemente: le regole aziendali si aggiornano, ma il database rimane stabile
- Gestiti da team diversi: DBA gestisce il DB, Data Governance gestisce le policy

**Esempio completo: una colonna `email`**
```
NEL DATABASE (SQL):
├─ Data type: VARCHAR(255)
├─ Is nullable: YES
├─ Primary Key: NO
└─ Foreign Key: NO

NEL DATA CATALOG (Alation/Collibra):
├─ Proprietario (Data Owner): Antonio Bianchi (Marketing)
├─ Significato: Email di contatto del cliente per comunicazioni di marketing
├─ GDPR sensitivity: SENSIBILE (diritto all'oblio, consenso richiesto)
├─ Qualità: 97% filled, 0.5% duplicati, valido come email
├─ Lineage: Salesforce → Sqoop → HDFS → Hive table → Report BI
├─ Retention: Cancellare dopo 3 anni di inattività (GDPR compliance)
├─ Usato in: 12 report BI, 3 model ML, Dashboard Executive
└─ Ultimo aggiornamento: 2 ore fa (batch notturno da Salesforce)
```

**Riassunto finale**

| Aspetto | Dove | Chi lo gestisce |
|---------|------|-----------------|
| **Dati effettivi** | Database (SELECT) | Developers, analisti |
| **Metadati tecnici** (schema, tipi, indici) | Database (information_schema) | DBA, SQL |
| **Metadati di business** (significato, ownership) | Data Catalog | Data Steward, Domain Expert |
| **Governance** (GDPR, lineage, audit, qualità) | Data Governance Platform | Compliance Officer, Data Governance |

**In una frase**: SQL gestisce la **struttura e i contenuti**, il Data Catalog gestisce il **significato e le regole aziendali**.

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

**Nota importante:**
- **Data Warehouse** (Hive, Snowflake) = esegue query SQL e governa tecnicamente
- **Data Catalog** (Alation, Collibra) = documenta significato business (NON esegue query)

