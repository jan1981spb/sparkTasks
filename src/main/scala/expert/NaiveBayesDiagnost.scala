package expert

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.io.File
import scala.reflect.io.Directory

class NaiveBayesDiagnost extends Diagnost {

  val spark: SparkSession = SparkSession.builder.master("local").getOrCreate()

  case class ConditionProbabilities(metricsWithStateP: Double = 1.0, noMetricWithStateP: Double = 1.0,
                                    metricsWithNoStateP: Double = 1.0, noMetricsNoStateP: Double = 1.0)

  calculateStateProbabilities(List("cpu_per_cluster", "response_delay_cluster", "response_delay_node"))

  def calculateStateProbabilities(metricsForCalc: List[String]): Unit = {
    import spark.implicits._

    deleteRecurcively("today")
    val metricsDf: DataFrame = readFromJdbc(table = "metrics")
    metricsDf.persist()

    val targetMetrics = metricsDf.select("snapshot_time", "node_id", "cpu_per_cluster", "response_delay_cluster",
      "response_delay_node", "state_id")

    saveDfToCsv(targetMetrics, "cluster/today", "metrics.csv")

    val statesDf: DataFrame = readFromJdbc(table = "states")

    // TODO Превратить count из действия в трансформацию
    val size: Long = metricsDf.count()

    val states = statesDf.collect().map(r => (r.getInt(0), r.getString(1)))

    val stateProbabilities: Array[Row] = states.map {
      state => {
        val stateId = state._1
        val stateName = state._2
        val aprioriStateP = 0.3
        val ConditionProbabilities(metricsWithStateP, noMetricWithStateP, metricsWithNoStateP, noMetricsNoStateP) = metricsForCalc
          .foldLeft(ConditionProbabilities()) {
            (accumulatedProbabilities, metric) =>
              // example metric = "cpu_per_cluster"
              val metricWithStateCount = metricsDf.where(metricsDf(metric) > 0 and metricsDf("state_id") === stateId).count()
              val metricWithStateP = (metricWithStateCount.toFloat / size.toFloat)
              val metricWithNoStateCount = metricsDf.where(metricsDf(metric) > 0 and !(metricsDf("state_id") === stateId)).count()
              val metricWithNoStateP = metricWithNoStateCount.toFloat / size.toFloat

              ConditionProbabilities(accumulatedProbabilities.metricsWithStateP * metricWithStateP,
                accumulatedProbabilities.noMetricWithStateP * metricWithNoStateP,
                accumulatedProbabilities.metricsWithNoStateP * (1 - metricWithStateP),
                accumulatedProbabilities.noMetricsNoStateP * (1 - metricWithNoStateP)
              )
          }

        val maxConditionStateP = (aprioriStateP * metricsWithStateP) / (aprioriStateP * metricsWithStateP + (1 - aprioriStateP) * noMetricWithStateP)
        val minConditionStateP = (aprioriStateP * metricsWithNoStateP) / (aprioriStateP * metricsWithNoStateP + (1 - aprioriStateP) * noMetricsNoStateP)
        Row(stateName, maxConditionStateP, minConditionStateP, aprioriStateP)
      }
    }

    val outputScheme = new StructType()
      .add(StructField("state", StringType, nullable = true))
      .add(StructField("max P", DoubleType, nullable = true))
      .add(StructField("min P", DoubleType, nullable = true))
      .add(StructField("apriori P", DoubleType, nullable = true))

    val result = spark.createDataFrame(spark.sparkContext.parallelize(stateProbabilities), outputScheme);

    deleteRecurcively("result")
    saveDfToCsv(result, "cluster/result", "diagnosis.csv")

    metricsDf.unpersist()
  }


  def readFromJdbc(table: String) = {
    spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", "jdbc:postgresql://localhost:5432/postgres")
      .option("dbtable", s"public.${table}")
      .option("user", "postgres")
      .option("password", "postgres")
      .load()
  }

  def deleteRecurcively(targetDir: String): Boolean = {
    val directory = new Directory(new File(s"/Users/andrej/IdeaProjects/sparkSandbox/src/main/resources/cluster/$targetDir"))
    directory.deleteRecursively()
  }

  def saveDfToCsv(df: DataFrame, targetDir: String, fileName: String): Unit = {
    df
      .write.format("csv")
      .option("header", "true")
      .save(s"/Users/andrej/IdeaProjects/sparkSandbox/src/main/resources/$targetDir")

    val metricsDirectory = new File(s"/Users/andrej/IdeaProjects/sparkSandbox/src/main/resources/$targetDir")

    if (metricsDirectory.exists && metricsDirectory.isDirectory) {
      val file = metricsDirectory.listFiles.filter(_.getName.endsWith(".csv")).head
      file.renameTo(new File(s"/Users/andrej/IdeaProjects/sparkSandbox/src/main/resources/$targetDir/$fileName"))
    }
  }

}
