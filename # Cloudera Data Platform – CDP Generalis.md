# CDP Generalist Exam – Guida Completa
## (CDP-0011)

**Dettagli Esame:**
- Numero domande: 60
- Pass Score: 60%
- Delivery: online, proctored
- Argomenti: 8 topic principali

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
- Gestisce namespace del file system
- Controlla metadata (nomi file, permessi, posizioni blocchi)
- Single point of failure (mitigato da HA)

DataNode (worker)
- Memorizza i blocchi di dati effettivi
- Invia heartbeat al NameNode
- Esegue letture/scritture su richiesta client
```

### 0.3 Concetti chiave HDFS

| Concetto | Descrizione |
|----------|-------------|
| **Blocco** | Unità minima di storage (default 128MB/256MB) |
| **Replica** | Numero di copie di ogni blocco (default 3) |
| **Rack Awareness** | Distribuisce repliche su rack diversi |
| **NameNode HA** | Secondary/Standby NameNode per failover |

👉 **Domanda tipica d'esame**
> HDFS è ottimizzato per? → **Grandi file, accesso sequenziale, throughput alto**
> Quante repliche default? → **3**

---

## 0.5 Hue – SQL Query Interface

### 0.5.1 Cos'è Hue

**Hue** è l'interfaccia web unificata per interrogare dati in CDP.

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
- API unificata: batch, streaming, ML, SQL, graph
- Supporta Scala, Java, Python, R

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
- Publish-subscribe messaging
- Storage persistente su disco
- Alta throughput (milioni msg/sec)
- Fault tolerance e replication

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

In **:contentReference[oaicite:0]{index=0}**, **Hive** e **Impala** non sono alternative, ma **complementari**.

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

**:contentReference[oaicite:1]{index=1}** è un **data warehouse distribuito** che fornisce:
- un livello SQL sopra Hadoop
- uno strato semantico sopra HDFS
- uno schema-on-read

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

Contiene:
- definizione delle tabelle
- schema delle colonne
- partizioni
- formati dei file
- location su HDFS / object storage

⚠️ **Punto d’esame cruciale**  
> Hive e Impala **condividono lo stesso Metastore**

Questo garantisce:
- coerenza semantica
- stessi dati, stesso schema
- governance centralizzata

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

👉 **Domanda tipica d’esame**  
> Quali tabelle sono consigliate per Data Lake? → **External**

---

## 2.6 Hive e performance

Hive è:
- **batch-oriented**
- adatto a scansioni complete
- meno performante su query rapide

Ottimizzazioni comuni:
- partizionamento
- formati colonnari (ORC, Parquet)
- predicate pushdown

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

