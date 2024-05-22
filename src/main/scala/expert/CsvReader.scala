package expert

import scala.annotation.tailrec
import scala.io.Source

class CsvReader {

  def parseSingleRecord(path: String): String = {
    val src = Source.fromFile(path)
    val iter  = src.getLines()
    iter.next()
  }

  def parseRecordsGroup(path: String, fieldIds: List[Int]): List[Array[String]] = {
    val src = Source.fromFile(path)
    val iter: Iterator[Array[String]] = src.getLines().drop(1).map(_.split(","))

    @tailrec
    def constructOutput(iter: Iterator[Array[String]], output: List[Array[String]]):List[Array[String]] = {
      if (iter.hasNext) {
        val sourceRecord = iter.next()
        val record = fieldIds.foldLeft(Array.empty[String]){
          (acc, id) => acc :+ sourceRecord.apply(id).toString
        }
        constructOutput(iter, output :+ record)
      } else output
    }

    constructOutput(iter, Nil)
  }

  //val rez = parseRecordsGroup("/Users/andrej/IdeaProjects/sparkSandbox/src/main/resources/cluster/today/metrics.csv", List(0,2))
 // println(rez)
}
