package sparkPratica

// sbt "runMain sparkPratica.obj_employee_tempview_dsl"
//
// Esempio con parametri modificabili:
// sbt "runMain sparkPratica.obj_employee_tempview_dsl minAge=50 educationField=Medical excludedJobRole=Sales Executive jobRoles=Manager|Research Director maxRows=30"
//
// Scopo dello script
// ------------------
// Questo script riprende il dataset employee train.csv e mostra due modi per
// interrogare gli stessi dati:
// - con una vista temporanea e Spark SQL;
// - con la DSL dei DataFrame, usando select, filter, ===, =!= e isin.
//
// I valori usati nei filtri sono centralizzati in EmployeeQueryParams e possono
// essere cambiati passando argomenti nel formato chiave=valore.
//
// Differenza rispetto a 11.apply.trasformation.scala
// --------------------------------------------------
// Il file 11 lavora sui dati country* senza header, ossia:
// - legge piu file countries* senza intestazione (Ossia senza nomi colonne);
// - definisce uno schema manuale con StructType e StructField;
// - pulisce colonne testuali con trim (per togliere spazi) e upper (per mettere tutto maiuscolo);
// - scrive output CSV su disco;
// - mostra partitionBy per creare cartelle partizionate per cntry_cd e language.
//
// Questo file 12 invece lavora sul dataset employee train.csv con header:
// - legge un CSV che contiene gia' i nomi delle colonne;
// - usa inferSchema per far dedurre a Spark i tipi delle colonne;
// - crea una vista temporanea con createOrReplaceTempView("employee");
// - interroga la vista con spark.sql, quindi con sintassi SQL;
// - ripete la stessa logica con la DataFrame DSL usando select e filter;
// - mostra operatori DSL come ===, =!= e isin;
// - usa parametri modificabili da args, per cambiare filtri senza riscrivere il codice.
//
// In sintesi:
// - il file 11 spiega schema manuale, pulizia e scrittura partizionata;
// - il file 12 spiega query SQL, vista temporanea e filtri con DataFrame DSL.

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import scala.util.Try

object obj_employee_tempview_dsl {
  private val DefaultInputPath = "file:///C:/repository/spark/1.input/train.csv"
  private val DefaultMaxRowsToShow = 20

  private val SelectedColumns = Seq(
    "Age",
    "BusinessTravel",
    "Department",
    "EducationField",
    "JobRole"
  )

  private case class EmployeeQueryParams(
      inputPath: String = DefaultInputPath,
      minAge: Int = 45,
      educationField: String = "Life Sciences",
      excludedJobRole: String = "Sales Executive",
      allowedJobRoles: Seq[String] = Seq("Research Scientist", "Manager"),
      maxRowsToShow: Int = DefaultMaxRowsToShow
  )

  private def printSection(title: String): Unit = {
    println()
    println("=" * 90)
    println(title)
    println("=" * 90)
  }

  private def printExplanation(text: String): Unit = {
    text.stripMargin.trim.linesIterator.foreach(line => println(line.trim))
  }

  private def showDataFrameDetails(title: String, df: DataFrame, maxRowsToShow: Int): Unit = {
    printSection(title)
    val totalRows = df.count()
    val rowsToShow = math.min(totalRows, maxRowsToShow).toInt

    println(s"Numero colonne: ${df.columns.length}")
    println(s"Colonne: ${df.columns.mkString(", ")}")
    println(s"Numero righe: $totalRows")
    println("Schema:")
    df.printSchema()
    println(s"Dati mostrati: $rowsToShow righe su $totalRows")
    df.show(rowsToShow, truncate = false)
  }

  private def parseParams(args: Array[String]): EmployeeQueryParams = {
    val options = args
      .flatMap { arg =>
        arg.split("=", 2) match {
          case Array(key, value) if key.trim.nonEmpty => Some(key.trim -> value.trim)
          case _ => None
        }
      }
      .toMap

    val defaults = EmployeeQueryParams()

    defaults.copy(
      inputPath = options.getOrElse("inputPath", defaults.inputPath),
      minAge = options.get("minAge").flatMap(value => Try(value.toInt).toOption).getOrElse(defaults.minAge),
      educationField = options.getOrElse("educationField", defaults.educationField),
      excludedJobRole = options.getOrElse("excludedJobRole", defaults.excludedJobRole),
      allowedJobRoles = options
        .get("jobRoles")
        .map(_.split("\\|").map(_.trim).filter(_.nonEmpty).toSeq)
        .filter(_.nonEmpty)
        .getOrElse(defaults.allowedJobRoles),
      maxRowsToShow = options.get("maxRows").flatMap(value => Try(value.toInt).toOption).getOrElse(defaults.maxRowsToShow)
    )
  }

  private def sqlString(value: String): String = {
    "'" + value.replace("'", "''") + "'"
  }

  private def printParams(params: EmployeeQueryParams): Unit = {
    printSection("Parametri usati")
    println(s"inputPath: ${params.inputPath}")
    println(s"minAge: ${params.minAge}")
    println(s"educationField: ${params.educationField}")
    println(s"excludedJobRole: ${params.excludedJobRole}")
    println(s"jobRoles per isin: ${params.allowedJobRoles.mkString(", ")}")
    println(s"maxRows: ${params.maxRowsToShow}")
  }

  def main(args: Array[String]): Unit = {
    val params = parseParams(args)

    printSection("AVVIO - Employee temp view e DataFrame DSL")
    printExplanation(
      """
        |Questo script legge train.csv con header e inferSchema.
        |Poi mette il DataFrame in cache per riutilizzarlo in piu punti.
        |
        |La stessa logica viene eseguita in due modi:
        |1. Spark SQL, dopo aver creato una vista temporanea chiamata employee;
        |2. DataFrame DSL, usando select, filter, ===, =!= e isin.
        |
        |I filtri sono parametri: se cambiano, puoi passarli da riga di comando.
      """
    )
    printParams(params)

    val conf = new SparkConf()
      .setAppName("employee-tempview-dsl")
      .setMaster("local[*]")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val employeeDf = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .load(params.inputPath)
      .cache()

    showDataFrameDetails("1 - Dataset employee letto da CSV", employeeDf, params.maxRowsToShow)

    printSection("2 - Creazione vista temporanea employee")
    printExplanation(
      """
        |createOrReplaceTempView registra il DataFrame come vista temporanea.
        |Da questo momento possiamo usare Spark SQL come se employee fosse una tabella.
        |La vista vive solo durante questa SparkSession.
      """
    )
    employeeDf.createOrReplaceTempView("employee")
    println("Vista temporanea creata: employee")

    printSection("3 - Query con Spark SQL")
    printExplanation(
      """
        |La query seleziona poche colonne e applica tre condizioni:
        |- Age maggiore del parametro minAge;
        |- EducationField uguale al parametro educationField;
        |- JobRole diverso dal parametro excludedJobRole.
      """
    )

    val selectedSqlColumns = SelectedColumns.mkString(", ")
    val sqlQuery =
      s"""
         |SELECT $selectedSqlColumns
         |FROM employee
         |WHERE Age > ${params.minAge}
         |  AND EducationField = ${sqlString(params.educationField)}
         |  AND JobRole <> ${sqlString(params.excludedJobRole)}
       """.stripMargin

    println("Query SQL eseguita:")
    println(sqlQuery.trim)

    val sqlResultDf = spark.sql(sqlQuery)
    showDataFrameDetails("Risultato Spark SQL", sqlResultDf, params.maxRowsToShow)

    printSection("4 - Query equivalente con DataFrame DSL")
    printExplanation(
      """
        |Con la DSL non scriviamo una stringa SQL.
        |Usiamo metodi del DataFrame:
        |- select per scegliere le colonne;
        |- filter per filtrare le righe;
        |- === per l'uguaglianza;
        |- =!= per il diverso.
      """
    )

    val dslResultDf = employeeDf
      .select(SelectedColumns.map(col): _*)
      .filter(
        col("Age") > params.minAge &&
          col("EducationField") === params.educationField &&
          col("JobRole") =!= params.excludedJobRole
      )

    showDataFrameDetails("Risultato DataFrame DSL", dslResultDf, params.maxRowsToShow)

    printSection("5 - Filtro DSL con isin")
    printExplanation(
      """
        |isin controlla se il valore della colonna e' presente in una lista.
        |In questo esempio manteniamo solo i JobRole indicati dal parametro jobRoles.
        |
        |Quando usiamo isin, il filtro JobRole =!= excludedJobRole diventa spesso ridondante:
        |se la lista contiene solo i ruoli ammessi, tutti gli altri ruoli vengono gia' esclusi.
      """
    )

    val isinResultDf = employeeDf
      .select(SelectedColumns.map(col): _*)
      .filter(
        col("Age") > params.minAge &&
          col("EducationField") === params.educationField &&
          col("JobRole").isin(params.allowedJobRoles: _*)
      )

    showDataFrameDetails("Risultato DataFrame DSL con isin su JobRole", isinResultDf, params.maxRowsToShow)

    printSection("FINE - Job completato")
    printExplanation(
      """
        |Lo script ha mostrato:
        |1. lettura CSV con header;
        |2. cache del DataFrame riutilizzato;
        |3. creazione di una vista temporanea;
        |4. query Spark SQL;
        |5. trasformazioni equivalenti con DataFrame DSL;
        |6. filtro su lista valori tramite isin.
      """
    )

    spark.stop()
  }
}
