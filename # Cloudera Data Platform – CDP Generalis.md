# Cloudera Data Platform – CDP Generalist (CDP-0011)
## Manuale di studio basato sul programma ufficiale d’esame

---

## Introduzione

Questo documento è una guida strutturata allo studio della **Cloudera Data Platform (CDP)**, costruita **sulla base del syllabus ufficiale del CDP Generalist Exam (CDP-0011)**.

Obiettivo:
- fornire una visione chiara dei **concetti richiesti all’esame**
- aiutare a **riconoscere i casi d’uso corretti**
- chiarire **differenze tra servizi e componenti**

Il documento **non sostituisce la documentazione ufficiale**, ma la rende più leggibile in ottica d’esame.

---

# 1. Componenti principali dell’architettura CDP

(≈ 15 domande d’esame)

---

## 1.1 HDFS (Hadoop Distributed File System)

HDFS è il file system distribuito utilizzato per:
- memorizzare grandi volumi di dati
- garantire affidabilità tramite replica
- supportare elaborazioni distribuite

Caratteristiche chiave:
- storage a blocchi
- replica (tipicamente 3)
- tolleranza ai guasti
- ottimizzato per throughput, non per latenza

Uso tipico:
- Data Lake
- storage per Hive, Impala, Spark

---

## 1.2 Hive e Impala

### Hive
Hive è un **data warehouse su Hadoop** utilizzato per:
- query SQL batch
- trasformazioni ETL
- analisi su grandi volumi di dati

Caratteristiche:
- schema-on-read
- latenza più elevata
- orientato a query complesse

### Impala
Impala è un **motore SQL interattivo** progettato per:
- bassa latenza
- query analitiche rapide
- accesso diretto ai file Hadoop

Differenza chiave (domanda tipica d’esame):
> Hive = batch  
> Impala = interattivo

---

## 1.3 Hue

Hue è una **interfaccia web** che fornisce:
- accesso SQL a Hive e Impala
- esplorazione tabelle
- editor query
- visualizzazione risultati

Hue **non è un motore di query**, ma un **tool di accesso**.

---

## 1.4 YARN

YARN è il **resource manager di Hadoop**.

Funzioni:
- allocazione CPU e memoria
- gestione job distribuiti
- supporto a più engine (Spark, Hive, MapReduce)

Componenti principali:
- ResourceManager
- NodeManager
- ApplicationMaster

---

## 1.5 Spark

Apache Spark è un **motore di elaborazione distribuito in memoria**.

Utilizzato per:
- ETL
- analytics avanzata
- machine learning

Caratteristica chiave:
- in-memory processing
- alte prestazioni su workload iterativi

---

## 1.6 Oozie

Apache Oozie è un **workflow scheduler** per Hadoop.

Permette di:
- concatenare job (Hive, Spark, MapReduce)
- gestire dipendenze
- schedulare flussi complessi

Domanda tipica:
> Oozie combina più job in un unico workflow logico.

---

## 1.7 Kafka

Apache Kafka è una **piattaforma di streaming**.

Utilizzato per:
- ingestione dati real-time
- messaging publish/subscribe
- pipeline event-driven

Caratteristiche:
- alta disponibilità
- throughput elevato
- persistenza dei messaggi

---

## 1.8 NiFi

Apache NiFi è un sistema per:
- ingestione dati
- routing
- trasformazione

Caratteristiche:
- interfaccia web
- programmazione visuale (no-code)
- controllo del flusso dei dati

---

## 1.9 HBase e Phoenix

### HBase
Database NoSQL per:
- accesso real-time
- letture/scritture random
- grandi volumi

### Phoenix
Layer SQL sopra HBase:
- consente query SQL
- traduce SQL in operazioni HBase

---

## 1.10 Kudu

Apache Kudu è uno storage colonnare progettato per:
- update frequenti
- accesso rapido
- integrazione con Impala

Caso d’uso tipico:
> analytics + aggiornamenti frequenti

---

# 2. Sicurezza in CDP Public Cloud e Private Cloud Base

(≈ 12 domande)

---

## 2.1 Shared Data Experience (SDX)

SDX è l’architettura di sicurezza e governance di Cloudera.

Include:
- Ranger (autorizzazione)
- Atlas (metadata e lineage)
- Hive Metastore
- Data Catalog
- Replication Manager
- Workload Manager

Concetto chiave:
> sicurezza e governance **coerenti su tutti i servizi**

---

## 2.2 Sicurezza in CDP Public Cloud

Caratteristiche:
- integrazione con Cloud SSO (SAML)
- uso dei servizi di sicurezza del cloud provider
- storage su S3 / ADLS / GCS

L’identity management è **federato**.

---

## 2.3 Sicurezza in CDP Private Cloud Base

Caratteristiche:
- integrazione con LDAP / Active Directory
- autenticazione Kerberos
- HDFS Transparent Encryption
- TLS per dati in transito

---

## 2.4 Cloudera Navigator Encrypt

Navigator Encrypt consente:
- cifratura dei dati a riposo
- senza modificare le applicazioni
- con impatto minimo sulle performance

---

# 3. Data Services (Experiences)

(≈ 9 domande)

---

## 3.1 Cloudera Data Engineering (CDE)

Servizio per:
- eseguire job Spark
- scheduling automatico
- cluster virtuali autoscalanti

Disponibile:
- Public Cloud
- Private Cloud Data Services

---

## 3.2 Cloudera Data Warehouse (CDW)

Servizio containerizzato per:
- data warehouse self-service
- workload isolati
- scalabilità indipendente

Supporta:
- Hive
- Impala

---

## 3.3 Cloudera Machine Learning (CML)

Servizio per:
- data science
- machine learning
- notebook (Python, R)

Unifica:
- data engineering
- data science

---

## 3.4 Cloudera Operational Database (COD)

Database operativo real-time:
- basato su HBase + Phoenix
- alta disponibilità
- bassa latenza

Caso d’uso tipico:
> autenticazione, profili utente, lookup veloci

---

## 3.5 Cloudera DataFlow (CDF)

Servizio basato su NiFi per:
- ingestione dati
- data streaming
- integrazione cloud-native

---

# 4. Deployment CDP Public Cloud

(≈ 9 domande)

---

## 4.1 Cloud supportati

CDP Public Cloud può essere deployato su:
- AWS
- Azure
- GCP

---

## 4.2 Environment in CDP Public Cloud

Un **environment** è:
- un sottoinsieme logico dell’account cloud
- associato a una rete virtuale
- riutilizzabile per più workload

Domanda tipica d’esame:
> Environment ≠ cluster

---

# 5. Deployment CDP Private Cloud Base

(≈ 6 domande)

---

## 5.1 CDP Private Cloud Base

Piattaforma installata:
- on-prem
- in data center privati

Include:
- Cloudera Runtime
- servizi core Hadoop

---

# 6. Cloudera Manager

(≈ 3 domande)

---

## 6.1 Funzioni principali

Cloudera Manager consente di:
- installare cluster
- configurare servizi
- monitorare salute
- gestire upgrade

Componenti:
- Server
- Agent

---

# 7. Workload XM

(≈ 3 domande)

---

## 7.1 Funzioni principali

Workload XM fornisce:
- visibilità sui workload
- analisi delle performance
- troubleshooting
- ottimizzazione job

Utilizza dati di telemetria.

---

# 8. Replication Manager

(≈ 3 domande)

---

## 8.1 Funzioni principali

Replication Manager consente:
- replica dati HDFS
- migrazione tra cluster
- replica verso Public Cloud

Supporta:
- HDFS
- Hive external tables
- HBase

---

## Conclusione

Questo documento copre **tutti gli argomenti ufficiali del CDP Generalist Exam (CDP-0011)**, organizzati per:
- componenti
- sicurezza
- servizi
- deployment
- strumenti di gestione

È ideale come:
- guida di studio
- ripasso pre-esame
- mappa concettuale CDP

