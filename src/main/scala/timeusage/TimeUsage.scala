package timeusage

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * The interface used by the grading infrastructure. Do not change signatures
 * or your submission will fail with a NoSuchMethodError.
 */
trait TimeUsageInterface {
  val spark: SparkSession
  def classifiedColumns(columnNames: List[String]): (List[Column], List[Column], List[Column])
  def row(line: List[String]): Row
  def timeUsageGrouped(summed: DataFrame): DataFrame
  def timeUsageGroupedSql(summed: DataFrame): DataFrame
  def timeUsageGroupedSqlQuery(viewName: String): String
  def timeUsageGroupedTyped(summed: Dataset[TimeUsageRow]): Dataset[TimeUsageRow]
  def timeUsageSummary(primaryNeedsColumns: List[Column], workColumns: List[Column], otherColumns: List[Column], df: DataFrame): DataFrame
  def timeUsageSummaryTyped(timeUsageSummaryDf: DataFrame): Dataset[TimeUsageRow]
}

/** Main class */
object TimeUsage extends TimeUsageInterface {
  val spark: SparkSession = SparkSession.builder()
    .appName("Time Usage")
    .master("local")
    .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    timeUsageByLifePeriod()
    spark.close()
  }

  def timeUsageByLifePeriod(): Unit = {
    val (columns, initDf) = read("src/main/resources/timeusage/atussum.csv")
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
    val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
    val finalDf = timeUsageGrouped(summaryDf)
    finalDf.show()
  }

  def read(path: String): (List[String], DataFrame) = {
    val df = spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv(path)
    (df.schema.fields.map(_.name).toList, df)
  }

  def classifiedColumns(columnNames: List[String]): (List[Column], List[Column], List[Column]) = {
    val primaryNeedsPattern = "^t(01|03|11|1801|1803)\\d{4}$".r
    val workPattern = "^t(05|1805)\\d{4}$".r

    val (primaryNeeds, work, other) = columnNames.foldLeft((List.empty[Column], List.empty[Column], List.empty[Column])) {
      case ((primaryNeedsCols, workCols, otherCols), colName) =>
        colName match {
          case primaryNeedsPattern() => (col(colName) :: primaryNeedsCols, workCols, otherCols)
          case workPattern() => (primaryNeedsCols, col(colName) :: workCols, otherCols)
          case _ => (primaryNeedsCols, workCols, col(colName) :: otherCols)
        }
    }

    (primaryNeeds.reverse, work.reverse, other.reverse)
  }

  def timeUsageSummary(primaryNeedsColumns: List[Column], workColumns: List[Column], otherColumns: List[Column], df: DataFrame): DataFrame = {
    if (df.isEmpty) {
      throw new IllegalArgumentException("Le DataFrame est vide. Veuillez vérifier les données d'entrée.")
    } else {
      val workingStatusProjection: Column =
        when($"telfs" >= 1 && $"telfs" < 3, "working")
          .otherwise("not working")
          .as("working")

      val sexProjection: Column =
        when($"tesex" === 1, "male")
          .otherwise("female")
          .as("sex")

      val ageProjection: Column =
        when($"teage" >= 15 && $"teage" <= 22, "young")
          .when($"teage" >= 23 && $"teage" <= 55, "active")
          .otherwise("elder")
          .as("age")

      val primaryNeedsProjection: Column =
        if (primaryNeedsColumns.isEmpty) lit(0.0).as("primaryNeeds")
        else (primaryNeedsColumns.reduce(_ + _) / 60.0).as("primaryNeeds")

      val workProjection: Column =
        if (workColumns.isEmpty) lit(0.0).as("work")
        else (workColumns.reduce(_ + _) / 60.0).as("work")

      val otherProjection: Column =
        if (otherColumns.isEmpty) lit(0.0).as("other")
        else (otherColumns.reduce(_ + _) / 60.0).as("other")

      df.select(
          workingStatusProjection,
          sexProjection,
          ageProjection,
          primaryNeedsProjection,
          workProjection,
          otherProjection
        )
        .where($"telfs" <= 4) // Discard people who are not in labor force
    }
  }

  def timeUsageGrouped(summed: DataFrame): DataFrame = {
    summed.groupBy("working", "sex", "age")
      .agg(
        round(avg($"primaryNeeds"), 1).as("primaryNeeds"),
        round(avg($"work"), 1).as("work"),
        round(avg($"other"), 1).as("other")
      )
      .orderBy("working", "sex", "age")
  }

  def row(line: List[String]): Row = {
    val values = line.map(_.toDouble).toArray
    Row.fromSeq(values)
  }

  def timeUsageGroupedSql(summed: DataFrame): DataFrame = {
    val viewName = s"summed"
    summed.createOrReplaceTempView(viewName)
    spark.sql(timeUsageGroupedSqlQuery(viewName))
  }

  def timeUsageGroupedSqlQuery(viewName: String): String =
    s"""
       |SELECT working, sex, age,
       |       ROUND(AVG(primaryNeeds), 1) AS primaryNeeds,
       |       ROUND(AVG(work), 1) AS work,
       |       ROUND(AVG(other), 1) AS other
       |FROM $viewName
       |GROUP BY working, sex, age
       |ORDER BY working, sex, age
       |""".stripMargin

  def timeUsageGroupedTyped(summed: Dataset[TimeUsageRow]): Dataset[TimeUsageRow] = {
    summed.groupByKey(row => (row.working, row.sex, row.age))
      .agg(
        round(avg($"primaryNeeds"), 1).as[Double],
        round(avg($"work"), 1).as[Double],
        round(avg($"other"), 1).as[Double]
      )
      .map {
        case ((working, sex, age), primaryNeeds, work, other) =>
          TimeUsageRow(working, sex, age, primaryNeeds, work, other)
      }
      .orderBy("working", "sex", "age")
  }

  def timeUsageSummaryTyped(timeUsageSummaryDf: DataFrame): Dataset[TimeUsageRow] =
    timeUsageSummaryDf.map { row =>
      TimeUsageRow(
        row.getAs[String]("working"),
        row.getAs[String]("sex"),
        row.getAs[String]("age"),
        row.getAs[Double]("primaryNeeds"),
        row.getAs[Double]("work"),
        row.getAs[Double]("other")
      )
    }
}

case class TimeUsageRow(
                         working: String,
                         sex: String,
                         age: String,
                         primaryNeeds: Double,
                         work: Double,
                         other: Double
                       )
