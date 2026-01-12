# Approfondimento – Apache Hive e Apache Impala
## (CDP Generalist Exam – CDP-0011)

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

