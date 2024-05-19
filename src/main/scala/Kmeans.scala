
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.util.Try

object Kmeans {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder()
      .appName("spark-app").master("local[*]").getOrCreate()
    session.sparkContext.setLogLevel("ERROR")

    import session.implicits._


    val df = Seq(
      (16, 1, 11), (9, 13, 8), (-9, -2, 1), (17, 12, 14), (8, 16, -19), (-2, 7, -29), (19, 7, 10), (4, 12, 5), (-10, -4, 3), (16, 15, 16), (6, 16, -17), (-4, 6, -32), (12, 7, 10), (4, 20, 7), (13, 6, 14), (1, 12, 6), (-9, -3, 0), (19, 11, 10), (4, 5, 10), (-5, 8, -28), (12, 5, 12), (3, 17, 7), (-10, 0, 4), (15, 16, 17), (7, 20, -14), (0, 0, -34), (19, 19, -19), (-1, 10, -25), (0, 8, 3), (7, 2, 0)
    ).toDF("x", "y", "z")

    val columnsNumber = df.columns.length
    val clustersNumber = 5
    val convergence = 5
    val neighboursNumber = 5
    val stepsLimit = 10

    def computeKanberrDistance(p: Row, center: Row): Double =
      (0 until center.length).foldLeft(0.0) { (acc, number) =>
        val diff = ((Math.abs(center.getInt(number) - p.getInt(number)) / (Math.abs(center.getInt(number)) + Math.abs(p.getInt(number))))).toDouble
        acc + Try(diff).fold({ case _: ArithmeticException => 0.0 }, v => v)
      }

    @scala.annotation.tailrec
    def createCluster(dfWithIndex: Array[Row], centroid: Row, centroidIndex: Int, step: Int): (Int, Seq[Row]) = {

      val neighboursWithDistances: Seq[(Row, Double)] = dfWithIndex.foldLeft(List((Row.empty, 0.0))) { (acc, trainRow) =>
        val dist = computeKanberrDistance(trainRow, centroid)
        val x = (trainRow, dist)
        acc :+ (x)
      }
        .sortBy(_._2).slice(1, neighboursNumber + 1)

      val means: Seq[Int] = (0 until columnsNumber).foldLeft(Seq.empty[Int])((sum, number) => {
        sum :+ (neighboursWithDistances.foldLeft(0)((sum, row) => sum + row._1.getInt(number)) / columnsNumber).toInt
      })

      val mean = Row.fromSeq(means)

      val diffCentroidFromMean = (0 until columnsNumber).foldLeft(0) {
        (acc, number) => {
          val diff = (Math.abs(centroid.getInt(number)) - Math.abs(mean.getInt(number)))
          acc + diff
        }
      }

      if (Math.abs(diffCentroidFromMean) > convergence) {
        createCluster(dfWithIndex, mean, centroidIndex, step + 1)
      } else {
        println(s"Для кластера $centroidIndex достигнута сходимость на шаге $step из $stepsLimit : значение $means")
        (centroidIndex, neighboursWithDistances.map(_._1))
      }
    }

    val window = Window.partitionBy("tmp_column").orderBy(lit('A'))
    val dfWithIndex = df.withColumn("tmp_column", lit('A')).withColumn("row_num", typedLit(row_number().over(window))).drop("tmp_column")
    dfWithIndex.show()
    dfWithIndex.persist()

    val totalSize = dfWithIndex.count().toInt

    val firstCentroids: Seq[Int] = ((0 until (totalSize)) by clustersNumber).toList.take(clustersNumber)

    print(firstCentroids)

    val dfCentroids: Dataset[Row] = dfWithIndex.where($"row_num".isin(firstCentroids: _*))
    dfCentroids.persist()
    val dfCentroidsArr = dfCentroids.collect()

    val testArrayWithoutCentroids = dfWithIndex.where(!$"row_num".isin(firstCentroids: _*)).collect()

    val rez: Array[(Int, Seq[Row])] = dfCentroidsArr.map(row => createCluster(testArrayWithoutCentroids, row, row.getInt(row.length - 1), 1))

    rez.foreach(v => println(v))

    dfWithIndex.unpersist()
    dfCentroids.unpersist()
    session.close()
  }


}
