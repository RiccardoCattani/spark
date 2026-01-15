# INTRODUZIONE: STORIA DI CLOUDERA E EVOLUZIONE VERSO CDP

## Pre-Cloudera: Il contesto tecnologico (2003-2008)

### Tecnologie pre-big data

Prima dell'avvento di Hadoop e Cloudera, il panorama del data management era dominato da soluzioni tradizionali.

**Data Warehouse tradizionali (anni '90-2000):**
- **Oracle Database** - leader enterprise RDBMS
- **IBM DB2** - mainframe e enterprise
- **Teradata** - appliance per analytics
- **Microsoft SQL Server** - piattaforma Windows

**Limitazioni critiche:**
- ❌ **Scalabilità verticale** - solo scale-up, hardware costoso
- ❌ **Gestione dati non strutturati** - difficile gestire log, testo come documenti articoli e commenti sui social media
- ❌ **Costi proibitivi** - milioni di dollari per petabyte
- ❌ **Schema rigido** - schema-on-write, no flessibilità
- ❌ **Vendor lock-in** - dipendenza da fornitori proprietari

## Differenza tra dati relazionali e non relazionali

### **Dati Relazionali**
I dati relazionali sono organizzati in un formato strutturato, seguendo uno schema rigido.

#### **Caratteristiche principali:**
1. **Struttura**:
   - Organizzati in tabelle con righe e colonne.
   - Ogni tabella ha uno schema predefinito (schema-on-write).
   - Le relazioni tra i dati sono definite tramite chiavi primarie e chiavi esterne.

2. **Esempi di dati**:
   - Informazioni su clienti (nome, cognome, email, telefono).
   - Transazioni finanziarie (ID transazione, importo, data).
   - Inventari di prodotti (ID prodotto, quantità, prezzo).

3. **Database relazionali (RDBMS)**:
   - MySQL, PostgreSQL, Oracle Database, Microsoft SQL Server.

4. **Vantaggi**:
   - **Integrità dei dati**: Garantita da vincoli (es. chiavi primarie, univocità).
   - **Query potenti**: Linguaggio SQL per interrogare i dati.
   - **Adatto a dati strutturati**: Ideale per applicazioni aziendali tradizionali.

5. **Svantaggi**:
   - **Scalabilità verticale**: Difficile scalare orizzontalmente (richiede hardware più potente).
   - **Schema rigido**: Cambiare lo schema può essere complesso.
   - **Non adatto a dati non strutturati**: Come immagini, video, log.

---

### **Dati Non Relazionali**
I dati non relazionali sono più flessibili e non seguono uno schema rigido.

#### **Caratteristiche principali:**
1. **Struttura**:
   - Non organizzati in tabelle (IMP).
   - Possono essere archiviati in formati come documenti, grafi, colonne o chiavi-valori.
   - Schema dinamico o assente (schema-on-read).

2. **Esempi di dati**:
   - Log di sistema (timestamp, messaggio di errore).
   - Post sui social media (testo, immagini, video).
   - Dati IoT (sensori, eventi in tempo reale).

3. **Database non relazionali (NoSQL)**:
   - MongoDB (documenti), Cassandra (colonne), Redis (chiavi-valori), Neo4j (grafi).

4. **Vantaggi**:
   - **Scalabilità orizzontale**: Aggiungere nodi per gestire più dati.
   - **Flessibilità**: Adatto a dati non strutturati o semi-strutturati.
   - **Performance**: Ottimizzato per specifici casi d'uso (es. letture/scritture rapide).

5. **Svantaggi**:
   - **Meno consistenza**: Non sempre garantisce transazioni ACID.
   - **Query limitate**: Non sempre supporta SQL.
   - **Meno adatto a dati strutturati**: Non ideale per applicazioni tradizionali.

---

HDFS (Hadoop Distributed File System) non è un database relazionale. È un file system distribuito progettato per archiviare grandi quantità di dati su cluster di computer. HDFS gestisce file e directory, non tabelle relazionali, e non impone uno schema rigido ai dati. Quindi, HDFS è considerato un sistema di archiviazione non relazionale.
Hive e Impala non sono database relazionali in senso stretto, ma sono motori di query che permettono di eseguire interrogazioni SQL su dati archiviati in HDFS (o altri file system distribuiti). Tuttavia, forniscono un’interfaccia relazionale: i dati sono organizzati in tabelle e si usa SQL per interrogarli, quindi si comportano come sistemi relazionali dal punto di vista dell’utente, pur non essendo veri e propri RDBMS tradizionali.

La differenza principale tra database e filesystem è la seguente:

Un filesystem gestisce l’archiviazione e l’organizzazione di file e cartelle su un disco. Permette di salvare, leggere, modificare e cancellare file, ma non offre funzionalità avanzate per la gestione strutturata dei dati.
Un database, invece, è progettato per archiviare, organizzare e gestire dati strutturati (ad esempio, in tabelle) e offre funzionalità come query, transazioni, integrità dei dati e sicurezza. Permette di cercare e manipolare i dati in modo efficiente tramite linguaggi come SQL.
In sintesi: il filesystem gestisce file, il database gestisce dati strutturati.


### **Confronto Tabellare**

| **Caratteristica**       | **Relazionale**                     | **Non Relazionale**               |
|---------------------------|-------------------------------------|----------------------------------|
| **Struttura**             | Tabelle (righe e colonne)          | Documenti, grafi, colonne, chiavi-valori |
| **Schema**                | Rigido (schema-on-write)*           | Flessibile (schema-on-read)       |
| **Scalabilità**           | Verticale                          | Orizzontale                       |
| **Adatto per**            | Dati strutturati                   | Dati non strutturati/semi-strutturati |
| **Esempi di database**    | MySQL, PostgreSQL, Oracle          | MongoDB, Cassandra, Neo4j         |
| **Query**                 | SQL                                | API specifiche o linguaggi NoSQL  |

* "Schema rigido on write" significa che, quando si scrivono dati in un database, questi devono rispettare una struttura (schema) predefinita e obbligatoria. Ogni record deve avere i campi, i tipi di dati e le regole stabilite dallo schema, altrimenti la scrittura viene rifiutata

In pratica:
- Prima di inserire dati, lo schema (ad esempio tabelle e colonne in SQL) deve essere già definito.
- Non puoi aggiungere dati con campi diversi o mancanti rispetto allo schema.
- Questo garantisce coerenza e integrità dei dati, ma riduce la flessibilità rispetto a sistemi con schema dinamico (come MongoDB).

---

In sintesi, i dati relazionali sono ideali per applicazioni aziendali tradizionali con dati strutturati, mentre i dati non relazionali sono più adatti per scenari moderni che richiedono flessibilità e scalabilità.

---

### **Concetto fondamentale: Database vs Schema**

#### **Spesso confusi, ma sono diversi:**

**Database (DB):**
- È l'**intero contenitore** di dati e metadati
- È il **livello più alto di organizzazione**
- Raggruppa più tabelle correlate (namespace Hive)
- Esempio: `hive_warehouse`, `analytics_db`

**Schema:**
- È la **struttura** di una singola tabella
- Definisce colonne, tipi di dati, vincoli
- È l'**intestazione** di una tabella
- Esempio: `(id INT, name STRING, salary DECIMAL)`

#### **Analogia visiva:**

```
┌─────────────────────────────────────────┐
│  Database: hive_warehouse               │  ← Intero cassetto
│                                         │
│  ┌───────────────────────────────────┐ │
│  │ Table: employees                  │ │
│  │ Schema: id INT, name STRING,      │ │  ← Struttura colonne
│  │         salary DECIMAL            │ │
│  │                                   │ │
│  │ Data (rows):                      │ │  ← Dati effettivi
│  │ 1, John, 50000                    │ │
│  │ 2, Jane, 60000                    │ │
│  └───────────────────────────────────┘ │
│                                         │
│  ┌───────────────────────────────────┐ │
│  │ Table: departments                │ │
│  │ Schema: dept_id INT,              │ │
│  │         dept_name STRING          │ │
│  └───────────────────────────────────┘ │
└─────────────────────────────────────────┘
```

#### **In SQL/Hive:**

```sql
-- Creare un DATABASE (contenitore)
CREATE DATABASE hive_warehouse;

-- Creare una TABELLA con SCHEMA definito (struttura)
CREATE TABLE hive_warehouse.employees (
    id INT,                    -- ↓ Questo è lo SCHEMA
    name STRING,               -- Definisce la struttura
    salary DECIMAL             -- della tabella
);

-- Inserire DATA (dati effettivi)
INSERT INTO employees VALUES (1, 'John', 50000);
```

#### **Differenze chiave:**

| **Aspetto**        | **Database**                      | **Schema**                          |
|-------------------|-----------------------------------|-------------------------------------|
| **Cos'è?**        | Contenitore/namespace            | Struttura di una singola tabella    |
| **Livello**       | Alto (raggruppa tabelle)         | Basso (singola tabella)             |
| **Comando SQL**   | `CREATE DATABASE mydb;`          | `CREATE TABLE mydb.mytable (...)`  |
| **Contiene**      | Molteplici tabelle               | Definizione colonne + tipi + vincoli |
| **Esempio**       | `analytics_db`, `sales_db`       | `(id INT, name STRING, date DATE)` |
| **Modifica**      | Raro (cambio struttura db)       | Più frequente (ALTER TABLE)        |

#### **Nel contesto Cloudera/Hive:**

- **Hive Database** = Namespace che raggruppa tabelle correlate
  ```sql
  CREATE DATABASE sales;  -- database per dati vendite
  CREATE TABLE sales.transactions (...);
  CREATE TABLE sales.customers (...);
  ```

- **Hive Schema** = Definizione della tabella (colonne + tipi)
  ```sql
  CREATE TABLE sales.transactions (
      transaction_id INT,
      customer_id INT,
      amount DECIMAL(10,2),
      transaction_date DATE
  );
  -- ↑ Questa è la struttura (schema) della tabella
  ```

#### **Per l'esame CDP:**

⚠️ Assicurati di capire:
- **Database**: namespace logico che organizza tabelle correlate
- **Schema**: la struttura fisica di una tabella (colonne + tipi)
- **Metastore Hive**: dove vengono archiviati i metadati (info su database e schema)
- **Atlas**: catalogo che documenta database, schema, lineage, ownership

---

### Google: La rivoluzione (2003-2004)

#### **Timeline chiara: Da Google a Hadoop**

**2003–2004: Google pubblica i paper rivoluzionari**

**Google File System (GFS) - 2003**
Google pubblicò un paper rivoluzionario sul **Google File System**.

**⚠️ Importante:** Nel 2003 **Hadoop non esisteva ancora**. Non si "parlava di Hadoop" mentre Google creava GFS.

**Problemi che GFS risolveva:**
- Storage distribuito su migliaia di server commodity
- Fault tolerance automatica
- Alta throughput per grandi file
- Gestione petabyte di dati web

**Paper:** "The Google File System" - Ghemawat, Gobioff, Leung (SOSP 2003)


**MapReduce (Google, 2004)**
- Google pubblica il paper "MapReduce: Simplified Data Processing on Large Clusters" (Dean, Ghemawat).
- Concetti chiave:
   - Map: elaborazione parallela dei dati
   - Reduce: aggregazione dei risultati
   - Scalabilità lineare: più nodi = più performance
   - Fault tolerance: retry automatico dei task falliti
- Google non rilascia codice open source, solo paper.

**2005–2006: Nasce Hadoop**
- Doug Cutting (su Apache Nutch) crea l’implementazione open source ispirata ai paper di Google:
   - HDFS (da GFS)
   - MapReduce (da Google MapReduce)
- Nel 2003 non esisteva Hadoop, ma si discutevano già i problemi di Big Data e scalabilità.
- GFS (Google File System) viene prima (2003), Hadoop nasce dopo (2006) come reimplementazione open source.

**Apache Hadoop: La nascita 2006**
- Doug Cutting e Mike Cafarella (ex Yahoo!) implementano Hadoop per creare un motore di ricerca web scalabile.
- Nome "Hadoop": elefante di peluche del figlio di Doug Cutting.
- Componenti iniziali: HDFS e MapReduce.
- 2006: Hadoop come sottoprogetto di Apache Nutch.
- 2008: Hadoop diventa progetto Apache top-level.

Def. attuale di Hadoop:  è un framework open source per l’archiviazione e l’elaborazione distribuita di grandi quantità di dati (Big Data) su cluster di computer. È composto principalmente da:

HDFS (Hadoop Distributed File System): file system distribuito che memorizza i dati su più nodi.
MapReduce: modello di programmazione per elaborare dati in parallelo.
Altri componenti: YARN (gestione delle risorse), Hive, Pig, ecc.
---

### Yahoo!: Il primo grande utilizzatore (2006-2008)

**Yahoo! assunse Doug Cutting nel 2006** per sviluppare Hadoop internamente.

**Motivi:**
- Indicizzare miliardi di pagine web
- Competere con Google nella ricerca web
- Ridurre costi infrastrutturali

**Investimenti Yahoo! in Hadoop:**
- Team dedicato di sviluppo
- Cluster da migliaia di nodi
- Contributi open source massicci
- Test in produzione su scala web

**Risultato:** Yahoo! dimostrò che Hadoop poteva scalare a livelli enterprise.

---

### Facebook: Big data sociale (2007-2008)

**Facebook** iniziò ad usare Hadoop per analytics sui dati utenti.

**Contributi di Facebook:**
- **Apache Hive** (2008) - SQL su Hadoop
- Processing di log massivi
- Analytics comportamentali utenti
- Data warehouse distribuito

**Jeff Hammerbacher** (co-fondatore di Cloudera) era il data team lead di Facebook.

---

### Il problema che Hadoop risolse

**Prima di Hadoop (2003-2006):**

```
┌─────────────────────────────────────────┐
│ Aziende con big data (Google, Yahoo!)   │
│                                         │
│ Opzioni:                                │
│ 1. Build custom distributed systems     │
│    → Costi: milioni $, anni sviluppo    │
│                                         │
│ 2. Buy expensive appliance              │
│    → Costi: $$$, no flessibilità        │
│                                         │
│ 3. Non fare analytics                   │
│    → Perdere insights business          │
└─────────────────────────────────────────┘
```

**Dopo Hadoop (2006-2008):**

```
┌─────────────────────────────────────────┐
│ Hadoop open source                      │
│                                         │
│ ✅ Commodity hardware (economico)       │
│ ✅ Scalabilità orizzontale infinita     │
│ ✅ Fault tolerance nativa               │
│ ✅ Open source (no vendor lock-in)      │
│ ✅ Dati non strutturati (log, testo)    │
│ ✅ Schema-on-read (flessibilità)        │
└─────────────────────────────────────────┘
```

---

### La sfida: Hadoop era difficile (2006-2008)

**Problemi di Hadoop "raw":**
- ❌ No gestione centralizzata (manuale)
- ❌ Installazione complessa
- ❌ Security minima (no autenticazione/autorizzazione)
- ❌ No monitoring/alerting
- ❌ No supporto enterprise
- ❌ Solo per esperti Linux/Java
- ❌ No governance/auditing

**Opportunità per Cloudera:**
Rendere Hadoop **enterprise-ready** con:
- Manager centralizzato
- Distribuzione pacchettizzata
- Security integrata
- monitoring/alerting
- Supporto professionale
- Governance e compliance

---

## 0.0 Come è nata Cloudera

### 0.0.1 Le origini (2008)

**Cloudera** è stata fondata nel 2008 da:
- Doug Cutting (creatore di Hadoop)
- Christophe Bisciglia (ex-Google)
- Amr Awadallah (ex-Yahoo!)
- Jeff Hammerbacher (ex-Facebook)

**Missione originale:** commercializzare Apache Hadoop e renderlo enterprise-ready.

---

## 0.1 Prima di CDP: CDH e HDP

### 0.1.1 Cloudera Distribution for Hadoop (CDH)

**CDH** (2009-2020) era la distribuzione Hadoop di Cloudera.

**Caratteristiche:**
- Componenti: Hadoop, HDFS, MapReduce, YARN, Hive, Impala, HBase, Spark
- Versioni: CDH 4, CDH 5, CDH 6
- Manager: Cloudera Manager (GUI per gestione cluster)
- Security: Cloudera Navigator (audit/lineage), Sentry (authorization)
- Deployment: on-premise (bare metal)

**Limitazioni:**
- Solo on-premise
- Architettura monolitica
- Scaling verticale/orizzontale limitato
- Sicurezza frammentata (tool separati)

---

### 0.1.2 Hortonworks Data Platform (HDP)

**Hortonworks** (fondata 2011) era il competitor principale di Cloudera.

**HDP** caratteristiche:
- 100% open source (no codice proprietario)
- Componenti: Hadoop, YARN, Hive, HBase, Kafka, Storm, Ranger, Atlas
- Manager: Ambari (GUI cluster management)
- Security: Ranger (authorization), Knox (gateway), Atlas (metadata)
- Philosophy: "enterprise Hadoop without vendor lock-in"

**Differenze CDH vs HDP:**

| Aspetto | CDH (Cloudera) | --------------------------| HDP (Hortonworks) |
|---------|----------------|-------------------------- |-------------------|
| Filosofia | Enterprise + alcune feature proprietarie | 100% open source |
| SQL Engine | Impala (proprietario, veloce)           | Hive + Tez |
| Manager | Cloudera Manager                           | Ambari |
| Security | Sentry + Navigator                        | Ranger + Knox + Atlas |
| Target | Enterprise con budget                       | Open source enthusiast |
| Support | Subscription commerciale                   | Subscription + community |

---

## 0.2 La fusione Cloudera (CDH) + Hortonworks (HDP) (2019)

### 0.2.1 Merger announcement (Ottobre 2018)

**Motivo della fusione:**
- Competizione cloud (AWS, Azure, GCP stavano dominando)
- Necessità di unire forze contro cloud provider
- Combinare best-of-breed di entrambe le piattaforme

**Nuovo nome:** Cloudera (mantiene il brand)

**Valore:** ~$5.2 miliardi USD

---

### 0.2.2 Cosa è cambiato dopo la fusione

✅ **Combinazione tecnologie:**
- Impala (da CDH) + Ranger/Atlas (da HDP)
- Cloudera Manager + features di Ambari
- Best security/governance di entrambe

✅ **Nuova vision:**
- Hybrid/multi-cloud (non solo on-premise)
- Enterprise Data Cloud
- Data lifecycle completo (ingest → process → serve → protect)

---

## 0.3 Nascita di CDP (Cloudera Data Platform)

### 0.3.1 Lancio CDP (2019-2020)

**CDP** è la piattaforma unificata che sostituisce CDH e HDP (Che prima erano unite).

**Novità rispetto a CDH/HDP:**
- ✅ **Hybrid cloud** (on-premise + AWS + Azure + GCP)
- ✅ **SDX integrato** (Shared Data Experience: security + governance unificate)
- ✅ **Containerizzazione** (Kubernetes per Data Services)
- ✅ **Separation of storage and compute** (invece di tight coupling)
- ✅ **Cloud-native architecture** (auto-scaling, serverless)
- ✅ **Data Services modulari** (CDE, CDW, COD, CML, CDF)

---

### 0.3.2 Versioni CDP

**CDP Private Cloud Base** (ex-CDH/HDP on-premise)
- Deployment: on-premise, bare metal
- Componenti: Cloudera Runtime (Hadoop, Spark, Hive, Impala, ecc.)
- Manager: Cloudera Manager
- Security: SDX (Ranger + Atlas integrati)
- Target: aziende con data center esistenti, compliance strict

**CDP Private Cloud Data Services** (nuovo, containerizzato, microservizi)
- Deployment: on-premise Kubernetes (OpenShift, ECS)
- Architettura: containerized, microservices
- Data Services: CDE, CDW, CML
- Target: aziende on-premise che vogliono cloud-like experience

**CDP Public Cloud** (cloud-native)
- Deployment: AWS, Azure, GCP
- Managed service (Cloudera gestisce control plane)
- Auto-scaling, elastic, pay-as-you-go
- Data Services: CDE, CDW, COD, CML, CDF
- Target: aziende cloud-first, workload variabili

---

## 0.4 Differenze architetturali: CDH/HDP → CDP

### 0.4.1 Differenze fisiche (infrastruttura)

| Aspetto | CDH/HDP (legacy) | CDP |
|---------|------------------|-----|
| **Deployment** | Bare metal on-premise | Hybrid: on-premise + cloud |
| **Architettura** | Monolitica (tutto su un cluster) | Modulare (Data Services separati) |
| **Storage/Compute** | Tightly coupled (HDFS locale) | Separated (S3/ADLS + compute elastico) |
| **Scaling** | Verticale/orizzontale hardware | Elastic cloud-native (auto-scaling) |
| **Containers** | No (bare metal JVMs) | Sì (Kubernetes) |
| **Hardware** | Commodity servers permanenti | Cloud VMs ephemeral |
| **Network** | Intra-cluster (rack-local) | Cloud VPC + cross-region |

---

### 0.4.2 Differenze logiche (software/architettura)

**Sicurezza e governance:**

| Aspetto | CDH | HDP | CDP |
|---------|-----|-----|-----|
| **Authorization** | Sentry | Ranger | Ranger (unified) |
| **Metadata/Lineage** | Navigator | Atlas | Atlas (unified) |
| **Gateway** | Knox (add-on) | Knox | Knox (integrated) |
| **Encryption** | Navigator Encrypt + HDFS TDE | HDFS TDE | HDFS TDE + cloud KMS |
| **Integrazione** | Tool separati | Tool separati | **SDX (tutto integrato)** |

**Management:**

| Aspetto | CDH | HDP | CDP |
|---------|-----|-----|-----|
| **Cluster manager** | Cloudera Manager | Ambari | Cloudera Manager (enhanced) |
| **Monitoring** | Cloudera Manager | Ambari + external | Cloudera Manager + Workload XM |
| **Replication** | Cloudera Manager | Ambari + Falcon | Replication Manager (unified) |

**Data Services (nuovo in CDP):**

CDH/HDP non avevano servizi modulari cloud-native.

CDP introduce:
- **CDE** (Cloudera Data Engineering) - Spark as a Service
- **CDW** (Cloudera Data Warehouse) - Hive/Impala virtual warehouses
- **COD** (Cloudera Operational DB) - HBase as a Service
- **CML** (Cloudera Machine Learning) - ML workspace unificato
- **CDF** (Cloudera DataFlow) - NiFi as a Service

Questi sono **containerizzati**, **auto-scaling**, **indipendenti**.

---

### 0.4.3 Storage: da HDFS locale a object storage

**CDH/HDP:**
```
Cluster on-premise
│
├── Nodo 1: HDFS DataNode + YARN NodeManager + Compute
├── Nodo 2: HDFS DataNode + YARN NodeManager + Compute
└── Nodo 3: HDFS DataNode + YARN NodeManager + Compute

Storage e compute sono ACCOPPIATI (tightly coupled)
```

**CDP Public Cloud:**
```
Cloud Storage (S3/ADLS/GCS) → Storage separato, persistente
          ↓
Ephemeral Compute Cluster (auto-scaling)
- Spark executors
- Hive/Impala workers
- Containers Kubernetes

Storage e compute sono SEPARATI (decoupled)
```

**Vantaggi separation of storage/compute:**
- ✅ Scale storage e compute indipendentemente
- ✅ Compute ephemeral (crea/distruggi cluster on-demand)
- ✅ Storage persistente (dati rimangono su S3/ADLS)
- ✅ Costo ridotto (paga solo compute quando serve)
- ✅ Durability cloud-native (11 nines su S3)

---

##### **1. CDP Public Cloud ☁️**
**✅ SÌ - Storage SEMPRE separato e SEMPRE in cloud**

```
┌─────────────────────────────────────────────────────────┐
│  CDP Public Cloud Architecture                          │
│                                                         │
│  ┌─────────────────┐         ┌──────────────────────┐  │
│  │  Compute Layer  │   ←→    │  Storage Layer       │  │
│  │  (Ephemeral)    │         │  (Persistent)        │  │
│  │                 │         │                      │  │
│  │ • CDE (Spark)   │         │ • S3 (AWS)           │  │
│  │ • CDW (Hive)    │         │ • ADLS (Azure)       │  │
│  │ • CML (ML)      │         │ • GCS (Google Cloud) │  │
│  │ • COD (HBase)   │         │                      │  │
│  │                 │         │ Object Storage       │  │
│  │ Auto-scaling    │         │ Durable, scalable    │  │
│  └─────────────────┘         └──────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

**Caratteristiche:**
- **Storage:** Object storage cloud (S3, ADLS Gen2, GCS)
- **Compute:** Cluster effimeri (EC2, Azure VMs, GCE instances)
- **Architettura:** Completamente disaccoppiata
- **Location:** Tutto in cloud (AWS/Azure/GCP)

**Vantaggi:**
- 🚀 **Elasticità totale:** scala compute senza toccare storage
- 💰 **Costi ottimizzati:** paghi compute solo quando lo usi
- 🔒 **Durabilità:** dati persistono anche cancellando cluster
- ♻️ **Multi-workload:** stessi dati accessibili da CDE, CDW, CML simultaneamente
- 🌍 **Global:** replica dati cross-region facilmente

**Esempio pratico:**
```
Data Lake su S3 (us-east-1)
    ↓
├─ CDE Virtual Cluster #1 (Spark batch)
│   └─ Scala/scompare on-demand
│
├─ CDW Virtual Warehouse (Impala query)
│   └─ Auto-scale basato su query load
│
└─ CML Workspace (data scientists)
    └─ Jupyter notebooks leggono/scrivono stesso Data Lake
```

---

##### **2. CDP Private Cloud Base 🏢**
**❌ NO - Storage e Compute ACCOPPIATI (architettura tradizionale Hadoop)**

```
┌─────────────────────────────────────────────────────────┐
│  CDP Private Cloud Base Architecture                    │
│                                                          │
│  Ogni nodo ha STORAGE + COMPUTE insieme                 │
│                                                          │
│  ┌──────────────────────────────────────────────────┐  │
│  │ Nodo 1: HDFS DataNode + YARN NodeManager        │  │
│  │         [Storage locale] + [Compute]             │  │
│  └──────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────┐  │
│  │ Nodo 2: HDFS DataNode + YARN NodeManager        │  │
│  │         [Storage locale] + [Compute]             │  │
│  └──────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────┐  │
│  │ Nodo 3: HDFS DataNode + YARN NodeManager        │  │
│  │         [Storage locale] + [Compute]             │  │
│  └──────────────────────────────────────────────────┘  │
│                                                          │
│  On-premise, bare metal servers                         │
└─────────────────────────────────────────────────────────┘
```

**Caratteristiche:**
- **Storage:** HDFS locale sui nodi del cluster
- **Compute:** YARN NodeManager sugli stessi nodi
- **Architettura:** Tightly coupled (heritage da CDH/HDP)
- **Location:** On-premise (data center aziendale)

**Perché è accoppiato:**
- 📍 **Data locality:** compute preferisce elaborare dati locali (stessa macchina)
- 🏗️ **Architettura legacy:** eredità da Hadoop originale (2006-2015)
- 🔧 **Hardware fisico:** server bare metal permanenti

**Limitazioni:**
- ⚠️ Non puoi scalare storage senza aggiungere nodi compute
- ⚠️ Non puoi scalare compute senza aggiungere storage HDFS
- ⚠️ Cluster sempre accesi (no elasticità on-demand)

---

##### **3. CDP Private Cloud Data Services 🏢☁️**
**✅ SÌ - Storage separato (on-premise ma cloud-like)**

```
┌─────────────────────────────────────────────────────────┐
│  CDP Private Cloud Data Services Architecture           │
│                                                          │
│  ┌─────────────────┐         ┌──────────────────────┐  │
│  │  Compute Layer  │   ←→    │  Storage Layer       │  │
│  │  (Kubernetes)   │         │  (Object Storage)    │  │
│  │                 │         │                      │  │
│  │ • CDE Pods      │         │ • Ozone              │  │
│  │ • CDW Pods      │         │ • MinIO              │  │
│  │ • CML Pods      │         │ • NetApp StorageGRID │  │
│  │                 │         │ • Dell ECS           │  │
│  │ OpenShift/ECS   │         │                      │  │
│  └─────────────────┘         └──────────────────────┘  │
│                                                          │
│  On-premise, but cloud-native architecture              │
└─────────────────────────────────────────────────────────┘
```

**Caratteristiche:**
- **Storage:** Object storage on-premise (Ozone, MinIO, NetApp, Dell ECS)
- **Compute:** Container orchestration (Kubernetes: OpenShift o ECS Anywhere)
- **Architettura:** Separata, cloud-native in data center
- **Location:** On-premise ma con modello cloud

**Vantaggi:**
- ✅ Separazione storage/compute come il cloud pubblico
- ✅ Auto-scaling dei Data Services
- ✅ Containerizzazione (portabilità)
- ✅ Rimane on-premise (compliance/data sovereignty)

---

##### **Tabella Riepilogativa**

| **CDP Deployment**               | **Storage Separato?** | **In Cloud?** | **Storage Type**        | **Compute Type**          |
|----------------------------------|-----------------------|---------------|-------------------------|---------------------------|
| **CDP Public Cloud**             | ✅ SÌ                 | ✅ SÌ         | S3/ADLS/GCS (object)   | Cloud VMs (ephemeral)     |
| **CDP Private Cloud Base**       | ❌ NO                 | ❌ NO         | HDFS (local disks)     | Bare metal (permanent)    |
| **CDP Private Cloud Data Services** | ✅ SÌ              | ❌ NO         | Ozone/MinIO (object)   | Kubernetes pods (elastic) |

---

##### **In sintesi (rispondendo alla tua domanda):**

**"In CDP lo storage è sempre separato dal compute ed in cloud?"**

**Risposta:**
- **CDP Public Cloud:** ✅ SÌ, sempre separato + sempre cloud (AWS/Azure/GCP)
- **CDP Private Cloud Base:** ❌ NO, accoppiato + on-premise (HDFS tradizionale)
- **CDP Private Cloud Data Services:** ✅ Separato ma ❌ on-premise (cloud-like architecture ma on prem)

**Quindi:**
- ✅ **Cloud = Sempre separato**
- ❌ **On-premise Base = Mai separato** (tightly coupled)
- ⚡ **On-premise Data Services = Separato ma non cloud** (hybrid model)

---

### 0.4.4 Da monolite a microservizi

**CDH/HDP (monolite):**
```
┌─────────────────────────────────────┐
│ Cluster unico on-premise            │
│ ┌─────────────────────────────────┐ │
│ │ HDFS + YARN + Hive + Spark +    │ │
│ │ HBase + Kafka + tutto insieme   │ │
│ └─────────────────────────────────┘ │
└─────────────────────────────────────┘
```
- Un cluster fa tutto
- Upgrade = downtime di tutto
- Resource contention tra workload

**CDP (microservizi):**
```
┌────────────────┐  ┌────────────────┐  ┌────────────────┐
│ CDE (Spark)    │  │ CDW (Hive)     │  │ CML (ML)       │
│ Auto-scaling   │  │ Auto-scaling   │  │ Isolated       │
└────────────────┘  └────────────────┘  └────────────────┘
         ↓                   ↓                   ↓
┌──────────────────────────────────────────────────────────┐
│ SDX (Shared Data Experience)                             │
│ Ranger + Atlas + Metastore                               │
└──────────────────────────────────────────────────────────┘
         ↓
┌──────────────────────────────────────────────────────────┐
│ Data Lake (S3/ADLS/GCS)                                  │
└──────────────────────────────────────────────────────────┘
```
- Servizi indipendenti
- Upgrade indipendenti (zero downtime)
- Isolamento risorse
- Governance condivisa (SDX)

---

## 0.5 Timeline evolutiva

```
2008 → Cloudera fondata
2009 → CDH 1 (prima distribuzione Hadoop commerciale)
2011 → Hortonworks fondata
2012 → CDH 4 (Impala lanciato)
2013 → HDP 2.0 (Ranger, Atlas)
2014 → CDH 5 (Spark integrato)
2017 → CDH 6 (ultima major release CDH)
2018 → Merger Cloudera + Hortonworks annunciato
2019 → Merger completato, CDP annunciato
2020 → CDP Public Cloud GA (General Availability)
2021 → CDP Private Cloud Data Services GA
2023 → CDP evoluzione continua (nuovi Data Services)
```

---

## 0.6 Perché CDP è importante per l'esame

**L'esame CDP-0011 testa:**
1. Conoscenza componenti (heritage da CDH/HDP)
2. Architettura moderna (cloud-native, SDX)
3. Differenze Public vs Private Cloud
4. Data Services (nuovo in CDP)

**Non chiede:**
- Dettagli su CDH/HDP legacy (obsoleti)
- Ambari (sostituito da Cloudera Manager)
- Sentry (sostituito da Ranger)

**Focus:** CDP come piattaforma unificata moderna.

---

## 0.7 Riepilogo: cosa devi sapere

✅ **CDP unisce best-of-breed di CDH e HDP**
✅ **SDX è il cuore (Ranger + Atlas + Metastore unificati)**
✅ **Hybrid cloud: Private Cloud Base + Public Cloud**
✅ **Separation of storage and compute (cloud-native)**
✅ **Data Services containerizzati e auto-scaling**
✅ **Security/governance integrate by design**

❌ **Non serve sapere:** dettagli CDH/HDP specifici, Ambari, Sentry

*SDX (Shared Data Experience) è una componente della piattaforma Cloudera che gestisce in modo centralizzato:
- la sicurezza, 
- il catalogo dei dati, 
- la governance 
- e le policy di accesso ai dati nei cluster Big Data

In pratica, SDX permette di:

- Definire e applicare regole di sicurezza e privacy sui dati.
- Gestire i metadati (informazioni sui dati) in modo unificato.
- Tracciare e monitorare chi accede ai dati e come vengono usati.
- Garantire coerenza e controllo su dati distribuiti tra diversi servizi (come Hadoop, Hive, Impala, ecc.).
- SDX semplifica la gestione dei dati in ambienti complessi, assicurando che le policy siano rispettate ovunque i dati vengano usati.

---

# PARTE 1: COMPONENTI PRINCIPALI CDP (15 domande)

---

## 0. HDFS – Hadoop Distributed File System

### 0.1 Cos'è HDFS

**HDFS** è un **file system distribuito Java-based** per memorizzare grandi volumi di dati.

**Caratteristiche principali:**
- Storage scalabile su cluster di commodity server
- Replica automatica dei dati (default 3 copie)
- Fault tolerance nativa
- Write-once, read-many (ottimizzato per streaming)

### 0.2 Architettura HDFS

```
NameNode (master)
- Gestisce namespace del file system* **
- Controlla metadata (nomi file, permessi, posizioni blocchi)
- Single point of failure (mitigato da HA)

DataNode (worker)
- Memorizza i blocchi di dati effettivi
- Invia heartbeat al NameNode
- Esegue letture/scritture su richiesta client
```

*“Gestisce namespace del file system” significa che il sistema (ad esempio HDFS) si occupa di organizzare e tenere traccia della struttura delle cartelle e dei file, dei loro nomi, delle gerarchie e dei percorsi. In pratica, il namespace è l’insieme di tutti i nomi (file e directory) e la loro organizzazione all’interno del file system, come una mappa che dice dove si trova ogni file o cartella.
**Il file system è il componente di un sistema operativo (o di una piattaforma come HDFS) che organizza, gestisce e memorizza i file e le cartelle su un dispositivo di archiviazione (come disco, SSD, ecc.). Permette di salvare, leggere, modificare e cancellare file, mantenendo la struttura gerarchica (cartelle, sottocartelle, percorsi) e gestendo i nomi e i permessi di accesso. In sintesi, è il “sistema” che tiene in ordine tutti i dati su un computer o cluster

### 0.3 Concetti chiave HDFS

| Concetto | Descrizione |
|----------|-------------|
| **Blocco** | Unità minima di storage (default 128MB/256MB) |
| **Replica** | Numero di copie di ogni blocco (default 3)   |
| **Rack Awareness** | Distribuisce repliche su rack diversi |
| **NameNode HA** | Secondary/Standby NameNode per failover  |

👉 **Domanda tipica d'esame**
> HDFS è ottimizzato per? → **Grandi file, accesso sequenziale, throughput alto**
> Quante repliche default? → **3**

---

## 0.5 Hue – SQL Query Interface

### 0.5.1 Cos'è Hue

**Hue** è l'interfaccia web unificata interrogare dati con hive ed impala in CDP.

**Funzioni principali:**
- Editor SQL per Hive e Impala
- File browser HDFS
- Job browser (YARN, Oozie)
- Query history e saved queries

### 0.5.2 Hue e integrazione motori

Hue si connette a:
- **Hive** per query batch
- **Impala** per query interattive
- **YARN** per monitoring job
- **Oozie** per workflow

👉 **Domanda tipica d'esame**
> Hue è un motore SQL? → **No, è un'interfaccia web per Hive/Impala**

---

## 0.7 YARN – Resource Manager

### 0.7.1 Cos'è YARN

**Apache Hadoop YARN** è il **resource manager** per applicazioni distribuite.

**Funzione principale:**
- Scheduling e allocazione risorse (CPU, RAM)
- Gestione container per applicazioni
- Monitoring e fault tolerance

### 0.7.2 Architettura YARN

```
ResourceManager (master)
- Scheduler globale
- Assegna risorse ai job

NodeManager (worker)
- Lancia container sui nodi
- Monitora utilizzo risorse

ApplicationMaster
- Negozia risorse per ogni applicazione
- Coordina esecuzione task
```

### 0.7.3 YARN e motori

YARN gestisce risorse per:
- MapReduce
- Spark
- Tez (Hive)

👉 **Domanda tipica d'esame**
> YARN gestisce storage o compute? → **Compute (CPU/RAM)**
> YARN è necessario per Impala? → **No, Impala è long-running daemon**

---

## 0.9 Apache Spark

### 0.9.1 Cos'è Spark

**Apache Spark** è un **motore di elaborazione distribuita in-memory** per big data e analytics.

**Caratteristiche:**
- Elaborazione in-memory (10-100x più veloce di MapReduce)
- API unificata: batch*, streaming, ML, SQL, graph
- Supporta Scala, Java, Python, R

* è un’operazione che esegue una serie di comandi o elaborazioni automaticamente, senza intervento umano, spesso su grandi quantità di dati.

### 0.9.2 Componenti Spark

| Componente | Funzione |
|------------|----------|
| **Spark Core** | Engine di base, RDD, task scheduling |
| **Spark SQL** | Query SQL su dati strutturati |
| **Spark Streaming** | Stream processing (micro-batch) |
| **MLlib** | Machine learning library |
| **GraphX** | Graph processing |

### 0.9.3 Spark vs MapReduce

| Aspetto | MapReduce | Spark |
|---------|-----------|-------|
| Storage intermedio | Disco (spill) | Memoria (RAM) |
| Latenza | Minuti | Secondi |
| Fault tolerance | Retry task | RDD lineage |
| API | Java MapReduce | Scala/Python/Java/R |

👉 **Domanda tipica d'esame**
> Spark è batch o streaming? → **Entrambi**
> Perché Spark è più veloce? → **Elaborazione in-memory**

---

## 0.11 Apache Oozie

### 0.11.1 Cos'è Oozie

**Apache Oozie** è un **workflow scheduler** per job Hadoop.

**Funzioni:**
- Orchestrazione job complessi (DAG - Directed Acyclic Graph)
- Scheduling basato su tempo o eventi
- Coordinamento dipendenze tra job

### 0.11.2 Tipi di job Oozie

| Tipo | Descrizione |
|------|-------------|
| **Workflow** | Sequenza di azioni (map, reduce, Spark, Hive, ecc.) |
| **Coordinator** | Workflow ricorrenti (schedule cron-like) |
| **Bundle** | Gruppi di coordinator |

### 0.11.3 Azioni supportate

Oozie supporta:
- MapReduce
- Spark
- Hive
- Sqoop
- Shell script
- Java

👉 **Domanda tipica d'esame**
> Oozie è un workflow scheduler? → **Sì**
> Combina job sequenzialmente? → **Sì**
> Supporta MiNiFi? → **No**

---

## 0.13 Apache Kafka

### 0.13.1 Cos'è Kafka

**Apache Kafka** è una **piattaforma di streaming distribuita** ad alte prestazioni.

**Caratteristiche:**
- Publish-subscribe messaging*
- Storage persistente su disco
- Alta throughput (milioni msg/sec)
- Fault tolerance e replication

*Publish-subscribe messaging: è un modello di comunicazione asincrona in cui i produttori (publisher) inviano messaggi a un canale o argomento (topic), senza conoscere i destinatari. I consumatori (subscriber) si iscrivono a uno o più topic e ricevono solo i messaggi di loro interesse. Questo modello permette un forte disaccoppiamento tra chi produce e chi consuma i dati, ed è molto usato in sistemi distribuiti, streaming e big data (es. Apache Kafka, RabbitMQ, Google Pub/Sub).

### 0.13.2 Concetti chiave Kafka

| Concetto | Descrizione |
|----------|-------------|
| **Topic** | Categoria/feed di messaggi |
| **Producer** | Pubblica messaggi su topic |
| **Consumer** | Legge messaggi da topic |
| **Broker** | Nodo Kafka che memorizza dati |
| **Partition** | Shard di un topic per parallelismo |

### 0.13.3 Kafka use cases

- Real-time streaming analytics
- Log aggregation
- Event sourcing
- Messaging tra microservizi

👉 **Domanda tipica d'esame**
> Kafka è storage o processing? → **Entrambi (memorizza + distribuisce)**
> Kafka è persistente? → **Sì, retention configurabile**

---

## 0.15 Apache NiFi

### 0.15.1 Cos'è NiFi

**Apache NiFi** è un **sistema per automatizzare il flusso di dati** tra sistemi.

**Caratteristiche:**
- GUI web drag-and-drop (visual programming)
- Connessioni a 300+ sorgenti/destinazioni
- Data provenance (tracciabilità completa)
- Backpressure handling

### 0.15.2 NiFi use cases

- Ingestione dati da sorgenti multiple
- Routing e trasformazione dati
- Data enrichment
- Push/pull da/verso sistemi esterni

### 0.15.3 NiFi vs Kafka

| Aspetto | NiFi | Kafka |
|---------|------|-------|
| Focus | Data flow orchestration | Messaging/streaming |
| GUI | Sì (visual) | No |
| Trasformazioni | Sì (native) | No (serve Kafka Streams) |
| Throughput | Medio/alto | Altissimo |

👉 **Domanda tipica d'esame**
> NiFi ha GUI? → **Sì, web-based drag-and-drop**
> NiFi è no-code? → **Sì, visual programming**

---

## 0.17 Apache HBase e Phoenix

### 0.17.1 Cos'è HBase

**Apache HBase** è un **database NoSQL distribuito** per accesso real-time a big data.

**Caratteristiche:**
- Modello wide-column (colonne sparse)
- Accesso random read/write veloce
- Scalabilità orizzontale automatica
- Consistency strong (non eventual)

### 0.17.2 HBase use cases

- Time-series data
- Real-time analytics
- Messaggistica e social media feed
- IoT sensor data

### 0.17.3 Cos'è Phoenix

**Apache Phoenix** è un **layer SQL sopra HBase**.

**Funzioni:**
- Query SQL su dati HBase
- Indici secondari
- JDBC driver
- Performance ottimizzate

👉 **Domanda tipica d'esame**
> HBase è relazionale? → **No, NoSQL wide-column**
> Phoenix cosa fa? → **SQL interface per HBase**

---

## 0.19 Apache Kudu

### 0.19.1 Cos'è Kudu

**Apache Kudu** è un **columnar storage engine** per Hadoop.

Kudu in Cloudera serve per memorizzare e gestire dati che devono essere sia letti che scritti velocemente, anche in tempo reale. È un database pensato per analisi veloci: permette di aggiungere, modificare e leggere dati subito, senza dover aspettare lunghi tempi di caricamento. Kudu è ideale quando hai bisogno di aggiornare spesso i dati e fare analisi rapide, ad esempio per dashboard, report o applicazioni che lavorano con dati sempre aggiornati.

**Caratteristiche:**
- Storage colonnare (come Parquet, ma mutabile)
- Fast analytics (scan) + fast updates/inserts
- Integrazione nativa con Impala e Spark
- ACID compliant

### 0.19.2 Kudu vs HBase vs HDFS

| Aspetto | HDFS | HBase | Kudu |
|---------|------|-------|------|
| Workload | Batch analytics | Random access | Hybrid (analytics + updates) |
| Updates | Immutabile | Veloce | Veloce |
| Scans | Veloce | Lento | Veloce |
| Formato | File-based | Row-based | Column-based |

👉 **Domanda tipica d'esame**
> Kudu è colonnare? → **Sì**
> Kudu supporta update? → **Sì, ACID**
> Kudu sostituisce HDFS? → **No, è complementare**

---

## 1. Ruolo di Hive e Impala nella Cloudera Data Platform

In **In cloudera**, **Hive** e **Impala** non sono alternative, ma **complementari**.

| Motore | Tipo di accesso | Caso d’uso principale |
|------|----------------|----------------------|
| Hive | SQL batch | ETL, reporting massivo |
| Impala | SQL interattivo | Analisi a bassa latenza |

👉 **Domanda tipica d’esame**  
> Quale scegliere per query interattive? → **Impala**  
> Quale per ETL batch? → **Hive**

---

## 2. Apache Hive – Approfondimento completo

## 2.1 Cos’è Apache Hive

**Apache Hive** è un **data warehouse distribuito** che fornisce:
- un livello SQL sopra Hadoop (Consente di scrivere query SQL-like per analizzare i dati in HDFS)
- è uno strato sopra HDFS (Hive organizza i dati in tabelle e schemi, fornendo una struttura logica ai file grezzi in HDFS)
- uno schema-on-read (Lo schema viene applicato ai dati solo quando vengono letti (non al momento della scrittura).

Hive **non è un database** e **non è OLTP**.

---

## 2.2 Hive come strato semantico del Data Lake

Senza Hive, il Data Lake è solo un insieme di file.  
Hive introduce:

- tabelle
- colonne
- tipi di dato
- partizioni

➡️ **Hive dà significato al dato**

Questo è fondamentale anche lato governance (Atlas, Ranger, auditing).

---

## 2.3 Hive Metastore (concetto CHIAVE per l’esame)

Il **Metastore** è il componente più importante di Hive.

### **Cosa memorizza il Metastore?**
1. **Definizione delle tabelle:**
   - Nomi delle tabelle e dei database.
2. **Schema delle tabelle:**
   - Colonne, tipi di dati, vincoli.
3. **Partizioni:**
   - Informazioni sulle partizioni delle tabelle (es. `year=2025`, `region=EU`).
4. **Formati dei file:**
   - Specifica il formato di archiviazione (es. Parquet, ORC, TextFile).
5. **Location su HDFS / Object Storage:**
   - Percorso fisico dei dati (es. `hdfs://data/employees`).
6. **Metadati aggiuntivi:**
   - Statistiche (es. numero di righe, dimensione dei dati).

### **Perché è importante per l’esame?**
- **Concetto chiave:** Il Metastore è il cuore di Hive, senza di esso non è possibile interrogare i dati.
- **Domande tipiche:**
  - Cosa memorizza il Metastore?
  - Dove sono archiviati i metadati di Hive?

### **Esempio pratico:**
Per una tabella Hive:
```sql
CREATE TABLE employees (
    id INT,
    name STRING,
    salary DECIMAL
)
PARTITIONED BY (department STRING)
STORED AS PARQUET
LOCATION 'hdfs://data/employees';
```
Il Metastore memorizza:
- **Schema delle colonne:** `id INT`, `name STRING`, `salary DECIMAL`.
- **Partizioni:** `department STRING`.
- **Formato file:** `PARQUET`.
- **Percorso:** `hdfs://data/employees`.

---

## 2.4 Schema-on-read (concetto fondamentale)

Hive applica lo schema **in lettura**, non in scrittura.

Vantaggi:
- ingestione rapida
- flessibilità
- adattabilità a sorgenti diverse

Svantaggi:
- errori di schema emergono a query time
- maggiore responsabilità sullo strato analitico

👉 **Domanda tipica d’esame**  
> Hive usa schema-on-read o schema-on-write? → **schema-on-read**

---

## 2.5 Tipi di tabelle Hive

### Managed Tables
- Hive gestisce dati e metadati
- `DROP TABLE` elimina anche i file
- più rischiose in ambienti enterprise

### External Tables
- Hive gestisce solo i metadati
- i dati restano esterni
- preferite nei Data Lake

👉 **Domanda tipica d'esame**  
> Quali tabelle sono consigliate per Data Lake? → **External**

---

## 2.6 Hive e performance

- Batch-oriented: Hive è progettato per elaborare grandi quantità di dati in blocco (batch), non per rispondere a singole query in tempo reale. È ideale per analisi massive, come report o aggregazioni su tabelle molto grandi.

- Adatto a scansioni complete: Hive lavora bene quando deve leggere e analizzare intere tabelle o grandi porzioni di dati, ad esempio per calcolare totali, medie o altre statistiche su dataset estesi.

- Meno performante su query rapide: Se hai bisogno di risposte immediate su pochi record (come una ricerca puntuale), Hive non è la scelta migliore, perché il suo motore (basato su MapReduce o Tez) ha una latenza di avvio elevata.

Ottimizzazioni comuni:
- Partizionamento: suddivide le tabelle in “parti” (es. per data, paese, ecc.), così le query leggono solo i dati necessari, riducendo i tempi di scansione.
- Formati colonnari (ORC, Parquet): questi formati memorizzano i dati per colonne invece che per righe, migliorando la compressione e la velocità di lettura per le query analitiche.
- Predicate pushdown: permette di applicare i filtri (WHERE) direttamente durante la lettura dei dati, evitando di caricare dati inutili e velocizzando le query.


---

## 2.7 Quando usare Hive (riassunto da esame)

Usa Hive quando:
- i dati sono molto grandi
- la latenza non è critica
- stai facendo ETL o reporting batch
- la priorità è la scalabilità

---

## 3. Apache Impala – Approfondimento completo

## 3.1 Cos’è Apache Impala

**:contentReference[oaicite:2]{index=2}** è un **motore SQL MPP (Massively Parallel Processing)** progettato per:
- query interattive
- bassa latenza
- analisi esplorativa

Impala **non usa MapReduce**.

---

## 3.2 Architettura di Impala

Impala utilizza:
- daemon su ogni nodo
- esecuzione in parallelo
- elaborazione in memoria

Caratteristiche:
- niente job batch
- niente scritture temporanee su HDFS
- risposta immediata

👉 **Domanda tipica d’esame**  
> Impala è batch o interattivo? → **interattivo**

---

## 3.3 Impala e Metastore condiviso

Impala:
- usa lo stesso Metastore di Hive
- vede le stesse tabelle
- usa gli stessi file su HDFS

⚠️ **Punto d’esame importante**
> Non esiste duplicazione dei dati tra Hive e Impala

---

## 3.4 Impala e performance

Impala è molto veloce perché:
- legge direttamente i file
- usa memoria
- sfrutta MPP

Ma:
- consuma molte risorse
- è sensibile a query inefficienti
- va governato (YARN, admission control)

---

## 3.5 Impala e sicurezza

In Cloudera, Impala:
- usa Kerberos
- applica policy Ranger
- è soggetto ad auditing

⚠️ Impala **espone dati velocemente** → rischio maggiore se mal configurato.

---

## 3.6 Quando usare Impala (riassunto da esame)

Usa Impala quando:
- serve risposta rapida
- analisi interattiva
- dashboard
- esplorazione dati

---

## 4. Confronto Hive vs Impala (TABELLA DA MEMORIZZARE)

| Caratteristica | Hive | Impala |
|--------------|------|--------|
| Tipo | Data Warehouse | SQL Engine |
| Latenza | Alta | Bassa |
| Uso | Batch / ETL | Interattivo |
| Motore | Job batch | MPP |
| Metastore | Sì | Sì (condiviso) |
| Schema | Schema-on-read | Schema-on-read |
| Query rapide | ❌ | ✅ |

👉 **Questa tabella copre il 90% delle domande Hive/Impala all’esame**

---

## 5. Scenario tipico d’esame (ragionamento)

**Domanda**  
Un analista deve eseguire query SQL rapide su grandi volumi di dati già strutturati. Quale strumento scegliere?

**Risposta corretta**
→ **Impala**

**Perché**
- bassa latenza
- SQL interattivo
- dati già nel Data Lake

---

## 6. Errori comuni da evitare all’esame

❌ Dire che Hive è interattivo  
❌ Dire che Impala è un data warehouse  
❌ Pensare che Hive e Impala abbiano storage separato  
❌ Confondere Metastore con HDFS  

---

## 7. Sintesi finale (da memorizzare)

- Hive = significato + batch
- Impala = velocità + interattività
- Metastore = cuore semantico
- HDFS = storage comune
- CDP = governance unica

---

## 8. Frase chiave da esame (memorizzala)

> **Hive e Impala sono due motori SQL diversi che condividono gli stessi dati e lo stesso Metastore, ma servono casi d’uso differenti.**
---
# PARTE 2: SICUREZZA CDP (12 domande)

## 9. Shared Data Experience (SDX)

### 9.1 Cos'è SDX

**SDX** è l'architettura di sicurezza e governance integrata di CDP.

**Componenti SDX:**
- **Apache Ranger** - Authorization & access control
- **Apache Atlas** - Metadata management & data lineage
- **Apache Knox** - Perimeter security (gateway)
- **Hive Metastore** - Schema centrale
- **Data Catalog** - Discovery & search
- **Replication Manager** - Backup & DR
- **Workload Manager** - Monitoring & optimization

### 9.2 Perché SDX è importante

✅ **Sicurezza integrata by design**
✅ **Governance centralizzata**
✅ **Policy consistenti su tutti i servizi**
✅ **Audit trail completo**

👉 **Domanda tipica d'esame**
> SDX include Ranger e Atlas? → **Sì**
> SDX è solo per security? → **No, anche governance e metadata**

---

## 10. Apache Ranger – Authorization

### 10.1 Cos'è Ranger

**Apache Ranger** fornisce **autorizzazione centralizzata** per l'ecosistema Hadoop.

**Funzioni:**
- Policy-based access control (PBAC)
- Role-based access control (RBAC)
- Fine-grained authorization (riga/colonna)
- Data masking (offuscamento dati sensibili)
- Row-level filtering
- Audit centralizzato

### 10.2 Servizi supportati da Ranger

Ranger gestisce accessi per:
- HDFS
- Hive
- Impala
- HBase
- Kafka
- Solr
- YARN
- Atlas

### 10.3 Ranger policies

**Tipi di policy:**
- **Access policies** - chi può fare cosa (SELECT, INSERT, UPDATE, DELETE)
- **Masking policies** - offusca dati sensibili (es. GDPR)
- **Row filter policies** - limita righe visibili per utente/gruppo

👉 **Domanda tipica d'esame**
> Ranger fa authentication o authorization? → **Authorization**
> Ranger supporta masking? → **Sì**

---

## 11. Apache Atlas – Metadata & Governance

### 11.1 Cos'è Atlas

**Apache Atlas** è la piattaforma di **metadata management e data governance**.

**Funzioni:**
- Metadata repository (catalogo dati)
- Data lineage (da dove viene il dato)
- Data classification (PII, sensibile, pubblico)
- Business glossary
- Search & discovery

### 11.2 Atlas e lineage

**Data Lineage** traccia:
- Origine dati (source)
- Trasformazioni (ETL)
- Destinazione (target)
- Dipendenze tra entità

Esempio: `Salesforce → Sqoop → HDFS → Hive → Impala → BI Report`

### 11.3 Atlas integration

Atlas traccia automaticamente:
- Hive queries (CREATE TABLE, INSERT)
- Spark jobs
- Sqoop imports
- NiFi flows

👉 **Domanda tipica d'esame**
> Atlas fa security o governance? → **Governance (metadata)**
> Atlas traccia lineage? → **Sì**

---

## 12. Apache Knox – Perimeter Security

### 12.1 Cos'è Knox

**Apache Knox** è un **gateway di sicurezza perimetrale** per cluster Hadoop.

**Funzioni:**
- Single point of access (reverse proxy)
- Authentication (LDAP, SSO, Kerberos)
- SSL/TLS termination
- Token-based authentication (JWT)
- Topology-based routing

### 12.2 Knox use cases

- Esporre servizi Hadoop (Hive, HBase) all'esterno del cluster
- Integrazione con enterprise SSO
- Protezione endpoint REST API
- Load balancing

👉 **Domanda tipica d'esame**
> Knox è un gateway? → **Sì**
> Knox fa authentication? → **Sì**

---

## 13. CDP Public Cloud – Integration SSO

### 13.1 Identity Federation

CDP Public Cloud supporta **identity federation** con SAML-based IdP.

**IdP supportati:**
- Okta
- Azure AD
- Google Workspace
- Ping Identity
- ADFS (Active Directory Federation Services)

### 13.2 Vantaggi SSO

✅ Single sign-on (un login per tutti i servizi)
✅ No account Cloudera necessario
✅ Gestione utenti centralizzata
✅ MFA (Multi-Factor Authentication) support

👉 **Domanda tipica d'esame**
> CDP Public supporta SAML SSO? → **Sì**
> Serve account Cloudera con SSO? → **No**

---

## 14. CDP Private Cloud – LDAP e Kerberos

### 14.1 Authentication in Private Cloud

CDP Private Cloud integra:
- **LDAP** (Lightweight Directory Access Protocol) per identity
- **Kerberos** per authentication
- **Active Directory** (LDAP + Kerberos combinati)

### 14.2 LDAP

**Funzioni:**
- User/group management
- Directory services
- Integrazione con AD aziendale

### 14.3 Kerberos

**Funzioni:**
- Strong authentication (ticket-based)
- Mutual authentication (client ↔ server)
- Protection contro replay attacks
- Single sign-on (SSO)

👉 **Domanda tipica d'esame**
> Private Cloud usa Kerberos? → **Sì**
> LDAP è per identity o authentication? → **Identity (Kerberos per auth)**

---

## 15. Encryption – Data at Rest

### 15.1 HDFS Transparent Encryption

**HDFS TDE** (Transparent Data Encryption):
- Cifratura automatica dei dati su HDFS
- Trasparente per applicazioni (no code change)
- Key management via KMS (Key Management Service)
- Per-zone encryption (encryption zones)

### 15.2 Cloudera Navigator Encrypt

**Navigator Encrypt**:
- Full-disk encryption per nodi del cluster
- Cifratura a livello OS (sotto HDFS)
- Protection anche se disco rubato
- Transparent per applicazioni

### 15.3 Cloud Storage Encryption

**Cloud providers:**
- **AWS S3** - SSE (Server-Side Encryption), KMS
- **Azure ADLS** - Storage Service Encryption
- **GCP GCS** - Default encryption at rest

👉 **Domanda tipica d'esame**
> HDFS supporta encryption at rest? → **Sì (TDE)**
> Cloud storage è cifrato? → **Sì (di default nei cloud provider)**

---

## 16. Encryption – Data in Transit

### 16.1 TLS/SSL

**Transport Layer Security** (TLS 1.2+):
- Cifratura traffico di rete
- Certificati X.509
- Protection contro man-in-the-middle

**Servizi con TLS:**
- HDFS (NameNode ↔ DataNode)
- Hive/Impala (client ↔ server)
- HTTP/REST API
- JDBC/ODBC connections

👉 **Domanda tipica d'esame**
> TLS cifra data in transit? → **Sì**
> CDP supporta TLS? → **Sì (1.2+)**

---

# PARTE 3: DATA SERVICES (9 domande)

## 17. Cloudera Data Engineering (CDE)

### 17.1 Cos'è CDE

**Cloudera Data Engineering** è un servizio per **gestire e schedulare job Apache Spark**.

**Caratteristiche:**
- Auto-scaling cluster Spark
- No overhead di gestione cluster manuale
- Supporto batch e streaming
- Integrato con SDX (security/governance)

### 17.2 CDE Public vs Private Cloud

| Aspetto | Public Cloud | Private Cloud |
|---------|--------------|---------------|
| Infrastructure | Cloud provider managed | On-premise Kubernetes |
| Scaling | Elastico (cloud-native) | Limitato da hardware |
| Deployment | Managed service | Self-service virtual cluster |

👉 **Domanda tipica d'esame**
> CDE è per Spark? → **Sì**
> CDE fa auto-scaling? → **Sì**

---

## 18. Cloudera Data Warehouse (CDW)

### 18.1 Cos'è CDW

**Cloudera Data Warehouse** è un servizio per creare **data warehouse indipendenti e auto-scaling**.

**Caratteristiche:**
- Containerizzato (Kubernetes)
- Hive e Impala virtual warehouses
- Auto-scaling dinamico
- Indipendenti e isolati (multi-tenancy)
- Upgrade indipendenti

### 18.2 CDW Virtual Warehouses

**Tipi:**
- **Hive Virtual Warehouse** - batch ETL
- **Impala Virtual Warehouse** - query interattive

Ogni VW può scalare indipendentemente.

### 18.3 CDW Public vs Private Cloud

| Aspetto | Public Cloud | Private Cloud |
|---------|--------------|---------------|
| Infrastructure | AWS/Azure/GCP | OpenShift/ECS/Kubernetes |
| Scaling | Elastic cloud-native | Limitato da capacity |
| Storage | S3/ADLS/GCS | HDFS/Ozone |

👉 **Domanda tipica d'esame**
> CDW è containerizzato? → **Sì (Kubernetes)**
> CDW supporta Hive e Impala? → **Sì (virtual warehouses)**

---

## 19. Cloudera Operational Database (COD)

### 19.1 Cos'è COD

**Cloudera Operational Database** è un servizio per **database operazionali real-time, scalabili e always-available**.

**Tecnologie:**
- Powered by **Apache HBase** (storage)
- **Apache Phoenix** (SQL interface)

### 19.2 COD use cases

- Low-latency random access
- Store/retrieve authentication details
- Time-series data (IoT, logs)
- Real-time analytics
- Session management

### 19.3 COD caratteristiche

✅ Auto-scaling
✅ Always-on (HA nativa)
✅ SQL via Phoenix
✅ NoSQL via HBase API

👉 **Domanda tipica d'esame**
> COD è per low-latency? → **Sì**
> COD usa HBase? → **Sì**
> COD è per authentication data? → **Sì (domanda campione esame)**

---

## 20. Cloudera Machine Learning (CML)

### 20.1 Cos'è CML

**Cloudera Machine Learning** unifica **data science e data engineering** in un servizio integrato.

**Funzioni:**
- Workspace collaborativo per data scientist
- Supporto Python, R, Scala
- Model training e deployment
- Jupyter, RStudio, VS Code integration
- MLOps (CI/CD per modelli)

### 20.2 CML caratteristiche

- **Workspace isolati** per progetti
- **Experiments tracking** (modelli, hyperparameter)
- **Model deployment** (REST API)
- **Model monitoring** (drift detection)

### 20.3 CML Public vs Private Cloud

| Aspetto | Public Cloud | Private Cloud |
|---------|--------------|---------------|
| Infrastructure | Cloud managed | OpenShift/Kubernetes |
| GPU support | Sì | Sì (se disponibile) |
| Scaling | Elastico | Limitato da cluster |

👉 **Domanda tipica d'esame**
> CML è per ML? → **Sì**
> CML supporta Python/R? → **Sì**

---

## 21. Cloudera DataFlow (CDF)

### 21.1 Cos'è CDF

**Cloudera DataFlow** è un servizio **cloud-native per distribuzione universale di dati**.

**Tecnologia:**
- Powered by **Apache NiFi**

**Caratteristiche:**
- Connect to any data source
- Process and deliver to any destination
- GUI drag-and-drop
- Auto-scaling flow deployments

### 21.2 CDF use cases

- Real-time data ingestion
- Data routing e enrichment
- Edge-to-cloud data movement
- IoT data collection

👉 **Domanda tipica d'esame**
> CDF usa NiFi? → **Sì**
> CDF è cloud-native? → **Sì (Public Cloud)**

---

# PARTE 4: DEPLOY CDP PUBLIC CLOUD (9 domande)

## 22. CDP Public Cloud – Concetti Core

### 22.1 Cos'è un Environment

**Environment** è un **subset logico del cloud provider account** che include:
- Virtual network (VPC/VNet)
- Security groups
- Storage locations (S3/ADLS/GCS)
- SDX (Data Lake)

✅ Puoi registrare **quanti environment vuoi**
✅ Ogni environment è isolato

👉 **Domanda tipica d'esame** (domanda campione esame)
> Environment è un subset del cloud account? → **Sì**
> Quanti environment posso creare? → **Quanti voglio**

---

## 22.2 Credential

**Credential** permette a CDP di **autenticarsi con il cloud provider**.

**Prerequisiti:**
- AWS: IAM role, cross-account access
- Azure: Service Principal, Managed Identity
- GCP: Service Account

---

## 22.3 Data Lake

**Data Lake** in CDP Public Cloud:
- Storage centralizzato (S3/ADLS/GCS)
- SDX (Ranger + Atlas)
- Hive Metastore
- Shared tra tutti i Data Hub dello stesso environment

---

## 23. CDP Public Cloud – Cloud Providers

### 23.1 AWS Requirements

**Prerequisites:**
- AWS account con privilegi sufficienti
- VPC con subnet (pubbliche/private)
- S3 bucket per storage
- IAM roles/policies
- Cross-account trust

**Services usati:**
- EC2 (compute)
- S3 (storage)
- RDS (Metastore/Ranger DB)
- EKS (Kubernetes per Data Services)

---

### 23.2 Azure Requirements

**Prerequisites:**
- Azure subscription
- Resource group
- VNet con subnet
- ADLS Gen2 storage account
- Service Principal o Managed Identity

**Services usati:**
- VMs (compute)
- ADLS Gen2 (storage)
- Azure Database (Postgres/MySQL)
- AKS (Kubernetes per Data Services)

---

### 23.3 GCP Requirements

**Prerequisites:**
- GCP project
- VPC network
- GCS bucket per storage
- Service Account

**Services usati:**
- Compute Engine (VMs)
- GCS (storage)
- Cloud SQL (databases)
- GKE (Kubernetes per Data Services)

---

👉 **Domanda tipica d'esame**
> CDP Public supporta AWS/Azure/GCP? → **Sì, tutti e tre**
> Serve VPC/VNet? → **Sì**

---

# PARTE 5: DEPLOY CDP PRIVATE CLOUD BASE (6 domande)

## 24. CDP Private Cloud Base – System Requirements

### 24.1 Hardware Requirements

**Per production cluster:**
- Minimum 3 nodi (5+ raccomandati)
- CPU: 4+ core per nodo (8+ raccomandati)
- RAM: 16GB+ per nodo (32GB+ raccomandati)
- Disk: 
  - OS: 100GB+
  - Data: dipende da workload (TB+)
- Network: 10 Gigabit Ethernet (raccomandato)

---

### 24.2 OS supportati

**Operating Systems:**
- Red Hat Enterprise Linux (RHEL) 7.x, 8.x
- CentOS 7.x, 8.x
- Ubuntu 18.04, 20.04
- SUSE Linux Enterprise Server (SLES) 12, 15

---

### 24.3 Java supportati

**Java Providers:** (domanda campione esame)
- ✅ **Oracle JDK** 8, 11
- ✅ **OpenJDK** 8, 11
- ✅ **Azul Zulu JDK** 8, 11
- ❌ Closed JDK (non esiste)

👉 **Domanda tipica d'esame** (domanda campione esame)
> Oracle JDK supportato? → **Sì**
> OpenJDK supportato? → **Sì**
> Azul/Zulu supportato? → **Sì**

---

### 24.4 Database supportati

**Databases per Metastore/Ranger:** (domanda campione esame)
- ✅ **PostgreSQL** 10, 11, 12
- ✅ **MySQL** 5.7, 8.0
- ✅ **MariaDB** 10.x
- ✅ **Oracle Database** 12c, 19c

👉 **Domanda tipica d'esame** (domanda campione esame)
> PostgreSQL supportato? → **Sì**
> MS SQL Server supportato? → **Sì**
> Oracle DB supportato? → **Sì**
> MySQL supportato? → **Sì**

---

### 24.5 Networking

**Requirements:**
- Forward and reverse DNS
- NTP (Network Time Protocol) sincronizzato
- Firewall rules per comunicazione inter-nodi
- No conflitti di porte

---

# PARTE 6: CLOUDERA MANAGER (3 domande)

## 25. Cloudera Manager

### 25.1 Cos'è Cloudera Manager

**Cloudera Manager** è l'applicazione per **gestire, configurare e monitorare cluster CDP Private Cloud Base**.

**Architettura:**
```

Cloudera Manager Server
- Gira su un host dedicato
- Web UI + API REST
- Gestisce uno o più cluster

Cloudera Manager Agent
- Gira su ogni nodo del cluster
- Esegue comandi dal Server
- Invia metriche e heartbeat
```

---

### 25.2 Funzioni principali

**Configurazione:**
- Install/upgrade servizi (Hive, HDFS, YARN, ecc.)
- Configurazione centralizzata
- Rolling restart

**Monitoring:**
- Metriche real-time (CPU, disk, network)
- Health checks automatici
- Alerts e notifications

**Troubleshooting:**
- Log aggregation
- Diagnostics
- Performance tuning

---

### 25.3 Cloudera Manager e database

Cloudera Manager richiede un database esterno per:
- Configuration metadata
- Monitoring data
- User/role management

**Databases supportati:** (già visto sopra)
- PostgreSQL
- MySQL / MariaDB
- Oracle DB
- MS SQL Server

👉 **Domanda tipica d'esame**
> Cloudera Manager gestisce cluster? → **Sì**
> Serve database? → **Sì**
> Dove gira? → **Su host dedicato**

---

# PARTE 7: WORKLOAD XM (3 domande)

## 26. Workload XM

### 26.1 Cos'è Workload XM

**Workload XM** è uno strumento per **monitorare, analizzare e ottimizzare workload** su cluster CDP.

**Funzioni:**
- Understand workloads (Hive, Impala, Spark)
- Troubleshoot failed jobs
- Optimize slow queries
- Capacity planning
- Cost optimization

---

### 26.2 Workload XM caratteristiche

**Analisi:**
- Query performance analysis
- Resource utilization (CPU, memory, I/O)
- Baseline comparison
- Anomaly detection

**Recommendations:**
- Query optimization suggestions
- Configuration tuning
- Resource allocation

---

### 26.3 Telemetry Publisher e redaction

**Telemetry Publisher** raccoglie dati diagnostici e li invia a Workload XM.

**Dati sensibili - redaction supportata:** (domanda campione esame)
- ✅ **Log and query** text
- ✅ **MapReduce job properties**
- ✅ **Spark event and executor log**
- ❌ HBase users (non redactable in questo contesto)
- ❌ Kafka topics (non redactable in questo contesto)

👉 **Domanda tipica d'esame** (domanda campione esame)
> Workload XM ottimizza query? → **Sì**
> Telemetry Publisher supporta redaction? → **Sì**
> Log and query possono essere redatti? → **Sì**

---

# PARTE 8: REPLICATION MANAGER (3 domande)

## 27. Replication Manager

### 27.1 Cos'è Replication Manager

**Replication Manager** è uno strumento per **replicare e migrare dati tra ambienti CDP**.

**Funzioni:**
- Copy HDFS data
- Replicate Hive external tables
- Backup HBase tables
- Disaster Recovery (DR)
- Cloud migration

---

### 27.2 Replication Manager – Private Cloud

**Private Cloud:**
- Replica HDFS tra cluster CDP Private Cloud Base 7.1.8+
- Replica Hive external tables
- Replica Ozone data

---

### 27.3 Replication Manager – Public Cloud

**Public Cloud:**
- Replica da CDH → CDP Public Cloud
- Replica da CDP Private Cloud Base → CDP Public Cloud
- Supporta HDFS, Hive, HBase
- Cloud storage (S3/ADLS/GCS) come destination

---

### 27.4 HDFS Replication Policies

**Requirements per replicare HDFS su cloud storage:** (domanda campione esame)
- ✅ **Register cloud credentials** in Replication Manager
- ✅ **Verify cluster access** e configure minimum ports
- ❌ Non serve configurare Hive (HDFS è indipendente)
- ❌ Non è vero che "works without configuration"

👉 **Domanda tipica d'esame** (domanda campione esame)
> Cosa serve per HDFS replication su cloud? → **Cloud credentials + cluster access**
> Serve configurare Hive? → **No (HDFS ≠ Hive)**

---

# RIEPILOGO FINALE

## Distribuzione domande per topic

| Topic | Domande | Focus |
|-------|---------|-------|
| **Componenti CDP** | 15 | HDFS, Hive, Impala, YARN, Spark, Oozie, Kafka, NiFi, HBase, Kudu |
| **Sicurezza** | 12 | SDX, Ranger, Atlas, Knox, encryption, SSO, Kerberos |
| **Data Services** | 9 | CDE, CDW, COD, CML, CDF |
| **Deploy Public Cloud** | 9 | AWS, Azure, GCP, environment, credential |
| **Deploy Private Cloud** | 6 | System requirements, Java, DB, OS |
| **Cloudera Manager** | 3 | Gestione cluster, configurazione |
| **Workload XM** | 3 | Monitoring, optimization, telemetry |
| **Replication Manager** | 3 | Backup, DR, migration |

**Totale:** 60 domande  
**Pass Score:** 60% (36 domande corrette su 60)

---

## Strategie per l'esame

### ✅ Cosa memorizzare

1. **Tabelle comparative** (Hive vs Impala, MapReduce vs Spark, ecc.)
2. **Use cases specifici** (quando usare quale strumento)
3. **Domande campione** (Oozie supporta MiNiFi? → No)
4. **Liste supportate** (Java providers, databases, cloud providers)
5. **Acronimi** (SDX, CDE, CDW, COD, CML, CDF, TDE, RBAC)

### ❌ Errori comuni da evitare

- Confondere Hive (batch) con Impala (interattivo)
- Pensare che Ranger faccia authentication (fa authorization)
- Credere che Atlas faccia security (fa governance/metadata)
- Confondere NiFi (data flow) con Kafka (messaging)
- Dimenticare che Hive e Impala condividono Metastore
- Non conoscere i database supportati da Cloudera Manager

### 📚 Risorse da studiare

1. **Questo documento** (coverage completa 60 domande)
2. **CDP documentation** ufficiale (link per approfondimenti)
3. **Cloudera Essentials for CDP** (corso video)
4. **Hands-on experience** (importante per domande pratiche)

---

## Frasi chiave da memorizzare

> **Hive e Impala** condividono dati e Metastore, ma servono casi d'uso differenti.

> **SDX** è l'architettura di sicurezza e governance integrata (Ranger + Atlas + Knox + Metastore).

> **COD** è ideal per low-latency, highly scalable storage/retrieval (authentication, IoT).

> **Environment** è un logical subset del cloud account con virtual network.

> **Oozie** è un workflow scheduler che combina job sequenzialmente.

> **Workload XM** ottimizza workload e troubleshoota failed jobs.

> **Replication Manager** replica dati tra ambienti CDP (serve cloud credentials + cluster access).

---

### **Cos'è lo Schema?**

Lo **schema** è una definizione strutturale che descrive l'organizzazione e il formato dei dati in un sistema di gestione dei dati. Esso specifica come i dati sono strutturati, quali campi o colonne esistono, e quali tipi di dati sono associati a ciascun campo.

#### **Caratteristiche principali dello Schema:**
1. **Definizione della struttura:**
   - Specifica i campi (colonne) e i loro tipi di dati (es. stringa, intero, data).
   - Definisce le relazioni tra i dati (es. chiavi primarie, univocità, ecc.) nei database relazionali.

2. **Tipi di Schema:**
   - **Schema-on-write:**
     - Lo schema è definito prima che i dati vengano scritti nel sistema.
     - Richiede che i dati rispettino la struttura predefinita.
     - Utilizzato nei database relazionali (es. MySQL, PostgreSQL).
   - **Schema-on-read:**
     - Lo schema viene applicato solo al momento della lettura o dell'analisi dei dati.
     - Permette di gestire dati non strutturati o semi-strutturati.
     - Utilizzato in strumenti di big data come Hadoop e Hive.

3. **Flessibilità:**
   - Nei sistemi relazionali, lo schema è rigido e deve essere rispettato.
   - Nei sistemi non relazionali, lo schema può essere flessibile o assente.

4. **Esempi di Schema:**
   - **Database relazionale:**
     ```sql
     CREATE TABLE Clienti (
       ID INT PRIMARY KEY,
       Nome VARCHAR(50),
       Cognome VARCHAR(50),
       Email VARCHAR(100)
     );
     ```
     Questo schema definisce una tabella con colonne e tipi di dati specifici.
   - **Big Data (Hive):**
     ```sql
     CREATE EXTERNAL TABLE LogDati (
       Timestamp STRING,
       Messaggio STRING
     )
     STORED AS TEXTFILE;
     ```
     Questo schema viene applicato ai dati già esistenti in HDFS.

### **Esempio di Schema**

Uno schema definisce la struttura di una tabella, specificando i nomi delle colonne e i tipi di dati associati. Ecco un esempio:

```sql
CREATE TABLE employees (
    id INT,          -- Identificativo univoco
    name STRING,     -- Nome del dipendente
    salary DECIMAL   -- Stipendio
);
```

#### **Dettagli dello schema:**
- **`id INT`**: Colonna `id` con tipo di dato intero (integer).
- **`name STRING`**: Colonna `name` con tipo di dato stringa (testo).
- **`salary DECIMAL`**: Colonna `salary` con tipo di dato decimale (numerico con precisione).

#### **Cosa rappresenta uno schema?**
- Lo schema è la **struttura logica** di una tabella.
- Specifica:
  1. **Nomi delle colonne** (es. `id`, `name`, `salary`).
  2. **Tipi di dati** (es. `INT`, `STRING`, `DECIMAL`).
  3. (Opzionale) **Vincoli** come chiavi primarie, univocità, ecc.

#### **Nota per l'esame CDP:**
- Ogni tabella in Hive o SQL richiede uno schema.
- Lo schema è obbligatorio per definire come i dati vengono archiviati e interrogati.

#### **Perché lo Schema è Importante?**
- **Organizzazione:** Permette di dare struttura ai dati, rendendoli più facili da interrogare e analizzare.
- **Validazione:** Garantisce che i dati rispettino determinati vincoli (es. tipi di dati corretti).
- **Governance:** Aiuta a mantenere coerenza e integrità nei sistemi di gestione dei dati.

---

In sintesi, lo schema è il "progetto" che descrive come i dati sono organizzati e strutturati in un sistema, influenzando il modo in cui vengono archiviati, letti e analizzati.

---

# Chiave primaria e chiave esterna

## Introduzione

In un database, l’organizzazione, l’identificazione e la coerenza dei dati dipendono sia dal **modello di database adottato** sia dall’uso corretto di **chiavi** e **metadati**.

In questo documento vengono spiegati:

* chiave primaria e chiave esterna nei database relazionali
* come vengono identificate tabelle e record
* le differenze tra **database relazionali (RDBMS)** e **non relazionali (NoSQL)**
* il collegamento con i sistemi **Big Data** come Hive

---

## Differenza tra database relazionali e non relazionali

### Database relazionali (RDBMS)

I database relazionali (**Relational Database Management System**) organizzano i dati in **tabelle** composte da **righe (record)** e **colonne (attributi)**, seguendo uno **schema fisso** definito a priori.

Ogni tabella:

* rappresenta un’**entità** del dominio applicativo
* è identificata dal **nome della tabella all’interno di uno schema**
* utilizza una **chiave primaria** per identificare univocamente i record
* può essere collegata ad altre tabelle tramite **chiavi esterne**

Le relazioni tra tabelle sono **esplicite** e il database garantisce la coerenza dei dati tramite le proprietà **ACID**:

* **Atomicità**
* **Consistenza**
* **Isolamento**
* **Durabilità**

📌 Esempi di RDBMS:

* MySQL
* PostgreSQL
* Oracle

Si chiamano database relazionali perché organizzano i dati in tabelle che possono essere messe in relazione tra loro tramite chiavi (primarie e esterne). Il termine “relazionale” deriva dal concetto matematico di “relazione”, che in questo contesto indica una tabella composta da righe e colonne. Le relazioni tra le tabelle permettono di collegare e integrare dati diversi in modo strutturato e coerente.

---

### Database non relazionali (NoSQL)

I database non relazionali (**NoSQL**) non si basano sul modello tabellare classico e **non richiedono uno schema rigido**. Sono progettati per gestire **grandi volumi di dati**, **alta scalabilità orizzontale** e dati **semi-strutturati o non strutturati**.

Non utilizzano chiavi esterne formali e le relazioni tra dati sono spesso:

* incorporate nei dati stessi
* oppure gestite a livello applicativo

I principali modelli NoSQL sono:

* **Documentali** (es. MongoDB)
  I dati sono memorizzati come documenti JSON/BSON. Ogni documento ha un identificatore univoco (es. `_id`), simile a una chiave primaria, ma senza vincoli relazionali.

* **Key-Value** (es. Redis)
  I dati sono coppie chiave–valore. Il modello è estremamente veloce, ma non supporta relazioni strutturate.

* **A colonne** (es. Cassandra, HBase)
  I dati sono organizzati per colonne e partizioni. È adatto a grandi volumi e carichi distribuiti.

* **A grafo** (es. Neo4j)
  I dati sono nodi e relazioni esplicite, ideali per rappresentare reti complesse (social network, recommendation system).

---

### Confronto sintetico RDBMS vs NoSQL

| Aspetto           | Database relazionali            | Database NoSQL                           |
| ----------------- | ------------------------------- | ---------------------------------------- |
| Modello dati      | Tabelle (righe/colonne)         | Documenti, key-value, colonne, grafi     |
| Schema            | Rigido                          | Flessibile o assente                     |
| Chiave primaria   | Sì (vincolo reale)              | Identificatore univoco                   |
| Chiave esterna    | Sì                              | No                                       |
| Coerenza          | Forte (ACID)                    | Eventuale (BASE)                         |
| Scalabilità       | Verticale                       | Orizzontale                              |
| Caso d’uso tipico | Transazioni, sistemi gestionali | Big Data, analytics, sistemi distribuiti |

---

## Cos’è una tabella

Una **tabella** rappresenta un’**entità** del mondo reale (ad esempio: Studente, Cliente, Ordine).
È composta da:

* **righe (record)** → singole istanze dell’entità
* **colonne (attributi)** → caratteristiche dell’entità

### Identificazione di una tabella

Una tabella **non è identificata dalla chiave primaria**.

👉 **La tabella è identificata dal suo nome all’interno di uno schema (o database).**

Formalmente, l’identificatore univoco di una tabella è:

```
(schema, nome_tabella)
```

Questo significa che due tabelle possono avere lo stesso nome se appartengono a schemi diversi.

### Esempio

```
public.studente
didattica.studente
```

Queste due tabelle:

* hanno lo stesso nome (`studente`)
* appartengono a schemi diversi
* sono **tabelle distinte**

---

## Chiave primaria (Primary Key)

### Definizione

La **chiave primaria** è un attributo (o un insieme di attributi) che **identifica univocamente ogni record di una tabella**.

👉 **La chiave primaria identifica un record, non la tabella.**

### Proprietà fondamentali

Una chiave primaria deve essere:

* **Univoca** (nessun duplicato)
* **Non nulla** (NOT NULL)
* **Stabile** (non dovrebbe cambiare nel tempo)

Può essere:

* **Semplice** → una sola colonna
* **Composta** → più colonne insieme

---

## Esempio pratico: chiave primaria

### Tabella STUDENTE

| matricola (PK) | nome  | cognome |
| -------------- | ----- | ------- |
| 12345          | Mario | Rossi   |
| 12346          | Luca  | Bianchi |

* `matricola = 12345` identifica **Mario Rossi**
* `matricola = 12346` identifica **Luca Bianchi**

---

## Chiave esterna (Foreign Key)

### Definizione

La **chiave esterna** è un attributo di una tabella che fa riferimento alla **chiave primaria di un’altra tabella**.

Serve a:

* collegare dati tra tabelle diverse
* garantire l’**integrità referenziale**

👉 Un valore di chiave esterna **deve corrispondere a una chiave primaria esistente**.

---

## Esempio pratico: chiave primaria e chiave esterna

### Tabella CLIENTE

| id_cliente (PK) | nome  |
| --------------- | ----- |
| 10              | Anna  |
| 11              | Paolo |

### Tabella ORDINE

| id_ordine (PK) | data       | id_cliente (FK) |
| -------------- | ---------- | --------------- |
| 501            | 2026-01-10 | 10              |
| 502            | 2026-01-12 | 10              |
| 503            | 2026-01-13 | 11              |

* `id_cliente` è **PK** in CLIENTE
* `id_cliente` è **FK** in ORDINE
* ogni ordine è associato a un cliente esistente

---

## Rappresentazione grafica concettuale

```
CLIENTE (1) ────────────< ORDINE (N)
```

* una riga di CLIENTE può essere collegata a molte righe di ORDINE
* la relazione è realizzata tramite la chiave esterna

---

## Chiave primaria composta

In alcuni casi, una singola colonna non è sufficiente a identificare un record.

### Esempio: ISCRIZIONE_ESAME

| matricola | id_esame |
| --------- | -------- |
| 12345     | 1        |
| 12345     | 2        |
| 12346     | 1        |

Qui la chiave primaria è **composta**:

```
PK = (matricola, id_esame)
```

Questa coppia identifica univocamente una singola iscrizione.

---

## Confronto con database non relazionali (NoSQL)

Nei database NoSQL:

* esiste solitamente un **identificatore univoco** del dato (es. `_id` in MongoDB)
* **non esistono chiavi esterne formali**
* le relazioni sono gestite dall’applicazione o tramite strutture annidate

### Esempio MongoDB

```
{
  _id: "u1",
  nome: "Anna",
  ordini: [501, 502]
}
```

* `_id` svolge un ruolo simile alla chiave primaria
* le relazioni non sono vincolate dal database

---

## Schema gerarchico di identificazione

```
Database
 └── Schema
      └── Tabella
           └── Record (riga)
```

* **Database** → identificato dal suo nome
* **Schema** → identificato dal suo nome all’interno del database
* **Tabella** → identificata dalla coppia *(schema, nome_tabella)*
* **Record** → identificato dalla **chiave primaria**

---

## Confronto: chiave primaria vs nome della tabella

| Aspetto                | Chiave primaria (PK)                    | Nome della tabella                     |
| ---------------------- | --------------------------------------- | -------------------------------------- |
| Cosa identifica        | Un **record (riga)**                    | Una **tabella**                        |
| Ambito                 | Interno alla tabella                    | All’interno di uno **schema/database** |
| Unicità                | Deve essere **univoca** per ogni record | Deve essere univoco nello schema       |
| Può cambiare?          | No (o fortemente sconsigliato)          | Sì (rinomina tabella)                  |
| Serve per le relazioni | Sì (referenziata dalle FK)              | No                                     |
| Esempio                | `id_cliente = 10`                       | `public.cliente`                       |

👉 **Errore comune da evitare**: pensare che la chiave primaria identifichi la tabella.

---

## Collegamento a Hive Metastore e Big Data

Nei sistemi Big Data basati su **Hive** (e più in generale su Hadoop/Cloudera), i concetti di database e tabella esistono ancora, ma il ruolo delle chiavi cambia.

### Hive Metastore

Il **Hive Metastore** è il servizio che mantiene i **metadati** delle tabelle, tra cui:

* database
* nome della tabella
* colonne e tipi di dato
* partizioni
* percorso fisico dei dati su HDFS o Object Storage

📌 In Hive:

* una tabella è identificata da **(database, nome_tabella)**
* esattamente come negli RDBMS con *(schema, nome_tabella)*

### Gerarchia in Hive

```
Hive Metastore
 └── Database
      └── Table
           └── Partition (opzionale)
                └── File / Record
```

### Chiavi primarie in Hive

* Hive **supporta sintatticamente** PRIMARY KEY e FOREIGN KEY
* **non sono vincoli applicati** (non viene garantita l’unicità)
* servono principalmente per:

  * documentazione del modello dati
  * ottimizzazioni del query planner
  * integrazione con strumenti di BI

👉 In Hive:

> **la chiave primaria non garantisce l’unicità dei record**, ma descrive l’intenzione logica del modello.

### Confronto RDBMS vs Hive

| Aspetto                | RDBMS                  | Hive / Big Data            |
| ---------------------- | ---------------------- | -------------------------- |
| Identità tabella       | (schema, nome)         | (database, nome)           |
| PK applicata           | Sì (vincolo reale)     | No (solo metadato)         |
| FK applicata           | Sì                     | No                         |
| Integrità referenziale | Garantita              | Demandata all’applicazione |
| Focus                  | Coerenza transazionale | Analisi su grandi volumi   |

### Collegamento concettuale chiave

* **RDBMS** → PK/FK = vincoli forti
* **Hive/Big Data** → PK/FK = informazione logica
* **Metastore** → “catalogo” delle tabelle, non dei record

-------|----------------------|-------------------|
| Cosa identifica | Un **record (riga)** | Una **tabella** |
| Ambito | Interno alla tabella | All’interno di uno **schema/database** |
| Unicità | Deve essere **univoca** per ogni record | Deve essere univoco nello schema |
| Può cambiare? | No (o fortemente sconsigliato) | Sì (rinomina tabella) |
| Serve per le relazioni | Sì (referenziata dalle FK) | No |
| Esempio | `id_cliente = 10` | `public.cliente` |

👉 **Errore comune da evitare**: pensare che la chiave primaria identifichi la tabella.

---

## Quando scegliere RDBMS, NoSQL o Hive

La scelta tra **database relazionali**, **NoSQL** e **Hive/Big Data** dipende principalmente dal **tipo di dati**, dal **carico di lavoro** e dai **requisiti di coerenza e scalabilità**.

---

### Quando scegliere un database relazionale (RDBMS)

Scegli un **RDBMS** quando:

* i dati sono **altamente strutturati**
* lo schema è **stabile nel tempo**
* sono richieste **transazioni affidabili**
* l’integrità dei dati è **critica**

📌 Casi d’uso tipici:

* sistemi gestionali (ERP, CRM)
* contabilità e fatturazione
* sistemi bancari e finanziari
* applicazioni OLTP

👉 Perché: PK e FK garantiscono **coerenza forte** e **integrità referenziale**.

---

### Quando scegliere un database NoSQL

Scegli un **NoSQL** quando:

* i dati sono **eterogenei o semi-strutturati**
* lo schema cambia frequentemente
* è richiesta **alta scalabilità orizzontale**
* le prestazioni sono più importanti della coerenza immediata

📌 Casi d’uso tipici:

* applicazioni web ad alto traffico
* caching e session management (Redis)
* IoT e time series
* sistemi distribuiti globali

👉 Perché: maggiore **flessibilità** e **scalabilità**, accettando coerenza eventuale.

---

### Quando scegliere Hive / Big Data

Scegli **Hive** (o sistemi Big Data simili) quando:

* i dati sono **molto voluminosi** (Big Data)
* il carico è principalmente **analitico** (OLAP)
* non servono transazioni riga-per-riga
* l’obiettivo è l’analisi storica e batch

📌 Casi d’uso tipici:

* data warehouse
* data lake
* reportistica e BI
* analisi su grandi dataset storici

👉 Perché: Hive separa **storage e compute** e scala su grandi volumi, ma non applica vincoli PK/FK.

---

### Confronto decisionale rapido

| Esigenza                | Tecnologia consigliata |
| ----------------------- | ---------------------- |
| Transazioni critiche    | RDBMS                  |
| Schema flessibile       | NoSQL                  |
| Altissimo volume dati   | Hive / Big Data        |
| Integrità referenziale  | RDBMS                  |
| Scalabilità orizzontale | NoSQL / Hive           |
| Analytics e BI          | Hive                   |

---

## Riassunto finale

* **La tabella rappresenta un’entità**
* **La chiave primaria identifica un record della tabella**
* **La chiave esterna collega record di tabelle diverse**
* Nei database relazionali, PK e FK garantiscono coerenza e integrità dei dati

👉 Frase chiave da ricordare:

> *La chiave primaria identifica univocamente una riga di una tabella; la chiave esterna realizza le relazioni tra tabelle.*

---

## Chiave primaria nei database non relazionali

Nei database non relazionali, il concetto di chiave primaria esiste ma può essere diverso rispetto ai database relazionali:
- Nei database documentali (es. MongoDB), ogni documento ha un identificatore unico (di solito il campo _id), che svolge il ruolo di chiave primaria.
- Nei database key-value, la “chiave” è sempre unica e identifica il valore associato.
- Nei database a colonne (es. Cassandra), si usano chiavi primarie composte per identificare in modo univoco le righe.
- Nei database a grafo, i nodi e le relazioni hanno identificatori unici.

Quindi, anche nei database non relazionali esiste un meccanismo per identificare univocamente i dati, ma la gestione e la struttura possono variare a seconda del modello.
