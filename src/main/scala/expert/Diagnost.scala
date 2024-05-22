package expert

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.io.File
import scala.annotation.tailrec

object Diagnost {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate()
    import spark.implicits._

    val df: DataFrame = spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", "jdbc:postgresql://localhost:5432/postgres")
      .option("dbtable", "public.metrics")
      .option("user", "postgres")
      .option("password", "postgres")
      .load()

    df.select("snapshot_time", "node_id", "cpu_per_cluster", "response_delay_cluster",
      "response_delay_node", "state_id")
      .write.format("csv")
      .option("header", "true")
      .save("/Users/andrej/IdeaProjects/sparkSandbox/src/main/resources/cluster/today")

    val activeApriorP = 0.6
    val failedApriorP = 0.3
    val degradedApriorP = 0.1

    df.persist()


    val size: Long = df.count()

    val cpuWithActiveCount = df.where(df("cpu_per_cluster") > 0 and df("state_id") === 21).count()
    val cpuWithActiveP = (cpuWithActiveCount.toFloat / size.toFloat)
    val cpuWithNoActiveCount = df.where(df("cpu_per_cluster") > 0 and !(df("state_id") === 21)).count()
    val cpuWithNoActiveP = cpuWithNoActiveCount.toFloat / size.toFloat

    val responseClusterWithActiveCount = df.where(df("response_delay_cluster") > 0 and df("state_id") === 21).count()
    val responseClusterWithActiveP = responseClusterWithActiveCount.toFloat / size.toFloat
    val responseClusterWithNoActiveCount = df.where(df("response_delay_cluster") > 0 and !(df("state_id") === 21)).count()
    val responseClusterWithNoActiveP = responseClusterWithNoActiveCount.toFloat / size.toFloat

    val nodeResponseWithActiveCount = df.where(df("response_delay_node") > 0 and df("state_id") === 21).count()
    val nodeResponseWithActiveP = nodeResponseWithActiveCount.toFloat / size.toFloat
    val nodeResponseWithNoActiveCount = df.where(df("response_delay_node") > 0 and !(df("state_id") === 21)).count()
    val nodeResponseWithNoActiveP = nodeResponseWithNoActiveCount.toFloat / size.toFloat

    val cpuWithFailCount = df.where(df("cpu_per_cluster") > 0 and df("state_id") === 23).count()
    val cpuWithFailP = cpuWithFailCount.toFloat / size.toFloat
    val cpuWithNoFailCount = df.where(df("cpu_per_cluster") > 0 and !(df("state_id") === 23)).count()
    val cpuWithNoFailP = cpuWithNoFailCount.toFloat / size.toFloat

    val responseClusterWithFailCount = df.where(df("response_delay_cluster") > 0 and df("state_id") === 23).count()
    val responseClusterWithFailP = responseClusterWithFailCount.toFloat / size.toFloat
    val responseClusterWithNoFailCount = df.where(df("response_delay_cluster") > 0 and !(df("state_id") === 23)).count()
    val responseClusterWithNoFailP = responseClusterWithNoFailCount.toFloat / size.toFloat

    val nodeResponseWithFailCount = df.where(df("response_delay_node") > 0 and df("state_id") === 23).count()
    val nodeResponseWithFailP = nodeResponseWithFailCount.toFloat / size.toFloat
    val nodeResponseWithNoFailCount = df.where(df("response_delay_node") > 0 and !(df("state_id") === 23)).count()
    val nodeResponseWithNoFailP = nodeResponseWithNoFailCount.toFloat / size.toFloat

    val cpuWithDegradedCount = df.where(df("cpu_per_cluster") > 0 and df("state_id") === 22).count()
    val cpuWithDegradedP = cpuWithDegradedCount.toFloat / size.toFloat
    val cpuWithNoDegradedCount = df.where(df("cpu_per_cluster") > 0 and !(df("state_id") === 22)).count()
    val cpuWithNoDegradedP = cpuWithNoDegradedCount.toFloat / size.toFloat

    val responseClusterWithDegradedCount = df.where(df("response_delay_cluster") > 0 and df("state_id") === 22).count()
    val responseClusterWithDegradedP = responseClusterWithDegradedCount.toFloat / size.toFloat
    val responseClusterWithNoDegradedCount = df.where(df("response_delay_cluster") > 0 and !(df("state_id") === 22)).count()
    val responseClusterWithNoDegradedP = responseClusterWithNoDegradedCount.toFloat / size.toFloat

    val nodeResponseWithDegradedCount = df.where(df("response_delay_node") > 0 and df("state_id") === 22).count()
    val nodeResponseWithDegradedP = nodeResponseWithDegradedCount.toFloat / size.toFloat
    val nodeResponseWithNoDegradedCount = df.where(df("response_delay_node") > 0 and !(df("state_id") === 22)).count()
    val nodeResponseWithNoDegradedP = nodeResponseWithNoDegradedCount.toFloat / size.toFloat

    @tailrec
    def makeDiagnose(cpuWithActiveP: Double, responseClusterWithActiveP: Double, nodeResponseWithActiveP: Double,
                     cpuWithNoActiveP: Double, responseClusterWithNoActiveP: Double, nodeResponseWithNoActiveP: Double,
                     cpuWithFailP: Double, responseClusterWithFailP: Double, nodeResponseWithFailP: Double,
                     cpuWithNoFailP: Double, responseClusterWithNoFailP: Double, nodeResponseWithNoFailP: Double,
                     cpuWithDegradedP: Double, responseClusterWithDegradedP: Double, nodeResponseWithDegradedP: Double,
                     cpuWithNoDegradedP: Double, responseClusterWithNoDegradedP: Double, nodeResponseWithNoDegradedP: Double,
                     count: Int
                    ): String = {

      // Общие условные вероятности для ACTIVE-статуса

      val activeCauseCommonP = cpuWithActiveP * responseClusterWithActiveP * nodeResponseWithActiveP
      val noActiveCauseCommonP = cpuWithNoActiveP * responseClusterWithNoActiveP * nodeResponseWithNoActiveP
      val activeCauseNoCommonP = (1 - cpuWithActiveP) * (1 - responseClusterWithActiveP) * (1 - nodeResponseWithActiveP)
      val noActiveCauseNoCommonP = (1 - cpuWithNoActiveP) * (1 - responseClusterWithNoActiveP) * (1 - nodeResponseWithNoActiveP)

      val maxActiveP = (activeApriorP * activeCauseCommonP) / (activeApriorP * activeCauseCommonP + (1 - activeApriorP) * (noActiveCauseCommonP))
      val minActiveP = (activeApriorP * activeCauseNoCommonP) / (activeApriorP * activeCauseNoCommonP + (1 - activeApriorP) * noActiveCauseNoCommonP)

      // Общие условные вероятности для FAIL-статуса

      val failCauseCommonP = cpuWithFailP * responseClusterWithFailP * nodeResponseWithFailP
      val noFailCauseCommonP = cpuWithNoFailP * responseClusterWithNoFailP * nodeResponseWithNoFailP
      val failCauseNoCommonP = (1 - cpuWithFailP) * (1 - responseClusterWithFailP) * (1 - nodeResponseWithFailP)
      val noFailCauseNoCommonP = (1 - cpuWithNoFailP) * (1 - responseClusterWithNoFailP) * (1 - nodeResponseWithNoFailP)

      val maxFailP = (failedApriorP * failCauseCommonP) / (failedApriorP * failCauseCommonP + (1 - failedApriorP) * (noFailCauseCommonP))
      val minFailP = (failedApriorP * failCauseNoCommonP) / (failedApriorP * failCauseNoCommonP + (1 - failedApriorP) * noFailCauseNoCommonP)

      // Общие условные вероятности для DEGRADED-статуса

      val degradedCauseCommonP = cpuWithDegradedP * responseClusterWithDegradedP * nodeResponseWithDegradedP
      val noDegradedCauseCommonP = cpuWithNoDegradedP * responseClusterWithNoDegradedP * nodeResponseWithNoDegradedP
      val degradedCauseNoCommonP = (1 - cpuWithDegradedP) * (1 - responseClusterWithDegradedP) * (1 - nodeResponseWithDegradedP)
      val noDegradedCauseNoCommonP = (1 - cpuWithNoDegradedP) * (1 - responseClusterWithNoDegradedP) * (1 - nodeResponseWithNoDegradedP)

      val maxDegradedP = (degradedApriorP * degradedCauseCommonP) / (degradedApriorP * degradedCauseCommonP + (1 - degradedApriorP) * (noDegradedCauseCommonP))
      val minDegradedP = (degradedApriorP * degradedCauseNoCommonP) / (degradedApriorP * degradedCauseNoCommonP + (1 - degradedApriorP) * noDegradedCauseNoCommonP)

      if (minActiveP > maxFailP && minActiveP > maxDegradedP) {
        s"Наиболее вероятно состоятие кластера ACTIVE: P min ACTIVE $minActiveP > P max FAILED $maxFailP и > P max DEGRADED $maxDegradedP, выявлено на шаге $count"
      } else if (minFailP > maxActiveP && minFailP > maxDegradedP) {
        s"Наиболее вероятно состоятие кластера FAILED: P min FAILED $minFailP > P max ACTIVE $maxActiveP и > P max DEGRADED $maxDegradedP, выявлено на шаге $count"
      } else if (minDegradedP > maxActiveP && minDegradedP > maxFailP) {
        s"Наиболее вероятно состоятие кластера DEGRADED: P min DEGRADED $minDegradedP > P max ACTIVE $maxActiveP и > P max FAILED $maxFailP, выявлено на шаге $count"
      } else {
        if (count == 0) {
          makeDiagnose(cpuWithActiveP, responseClusterWithActiveP = 1, nodeResponseWithActiveP,
            cpuWithNoActiveP, responseClusterWithNoActiveP = 1, nodeResponseWithNoActiveP,
            cpuWithFailP, responseClusterWithFailP = 1, nodeResponseWithFailP,
            cpuWithNoFailP, responseClusterWithNoFailP = 1, nodeResponseWithNoFailP,
            cpuWithDegradedP, responseClusterWithDegradedP = 1, nodeResponseWithDegradedP,
            cpuWithNoDegradedP, responseClusterWithNoDegradedP = 1, nodeResponseWithNoDegradedP,
            count + 1)
        } else if (count == 1) {
          makeDiagnose(cpuWithActiveP, responseClusterWithActiveP = 1, nodeResponseWithActiveP = 1,
            cpuWithNoActiveP, responseClusterWithNoActiveP = 1, nodeResponseWithNoActiveP = 1,
            cpuWithFailP, responseClusterWithFailP = 1, nodeResponseWithFailP = 1,
            cpuWithNoFailP, responseClusterWithNoFailP = 1, nodeResponseWithNoFailP = 1,
            cpuWithDegradedP, responseClusterWithDegradedP = 1, nodeResponseWithDegradedP = 1,
            cpuWithNoDegradedP, responseClusterWithNoDegradedP = 1, nodeResponseWithNoDegradedP = 1,
            count + 1)
        } else {
          "Недостаточно данных для диагностики, требуются дополнительные наблюдения"
        }
      }
    }

    val diagnose = makeDiagnose(cpuWithActiveP, responseClusterWithActiveP, nodeResponseWithActiveP,
      cpuWithNoActiveP, responseClusterWithNoActiveP, nodeResponseWithNoActiveP,
      cpuWithFailP, responseClusterWithFailP, nodeResponseWithFailP,
      cpuWithNoFailP, responseClusterWithNoFailP, nodeResponseWithNoFailP,
      cpuWithDegradedP, responseClusterWithDegradedP, nodeResponseWithDegradedP,
      cpuWithNoDegradedP, responseClusterWithNoDegradedP, nodeResponseWithNoDegradedP,
      count = 0
    )

    println(s"Диагноз: $diagnose")

    val result = Seq(s"Диагноз $diagnose").toDF()

    result
      .repartition(1)
      .write
      .format("csv")
      .save("/Users/andrej/IdeaProjects/sparkSandbox/src/main/resources/cluster/result")

    val directory = new File("/Users/andrej/IdeaProjects/sparkSandbox/src/main/resources/cluster/result")

    if (directory.exists && directory.isDirectory) {
      val file = directory.listFiles.filter(_.getName.endsWith(".csv")).head
      file.renameTo(new File("/Users/andrej/IdeaProjects/sparkSandbox/src/main/resources/cluster/result/diagnosis.csv"))
    }

  }

}
