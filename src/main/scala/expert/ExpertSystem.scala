package expert

import com.typesafe.config.{Config, ConfigFactory, ConfigList}
import scalafx.application.JFXApp3
import scalafx.application.JFXApp3.PrimaryStage
import scalafx.collections.ObservableBuffer
import scalafx.scene.Scene
import scalafx.scene.control.{TableColumn, TableView, TextArea}
import scalafx.scene.control.TableColumn._
import scalafx.scene.layout.HBox

import java.util
import scala.collection.JavaConverters.asScalaSetConverter

object ExpertSystem
  extends JFXApp3 {

  val diagnost = new NaiveBayesDiagnost()
  val config: Config = ConfigFactory.load().getConfig("metrics")
  val features = config.root.keySet.asScala.map(key ⇒ key → config.getString(key)).toMap

  // diagnost.calculateStateProbabilities(features.filter { case (_, value) => value == "true" }.keys.toList)

  private val metrics = ObservableBuffer[Metrics]()
  val parser = new CsvReader()
  val records: Seq[Array[String]] = parser.parseRecordsGroup("/Users/andrej/IdeaProjects/sparkSandbox/src/main/resources/cluster/today/metrics.csv", List(0, 1, 2, 3, 4, 5))

  records.foreach {
    record => metrics += new Metrics(record.apply(0), record.apply(1), record.apply(2), record.apply(3), record.apply(4), record.apply(5))
  }

  private val diagnosisResults = ObservableBuffer[DiagnosticResult]()
  val diagnostics: Seq[Array[String]] = parser.parseRecordsGroup("/Users/andrej/IdeaProjects/sparkSandbox/src/main/resources/cluster/result/diagnosis.csv", (0 until  4).toList)

  diagnostics.foreach {
    diagnosis => diagnosisResults += new DiagnosticResult(diagnosis.apply(0), diagnosis.apply(1), diagnosis.apply(2), diagnosis.apply(3))
  }

  override def start(): Unit = {
    val title = "Данные и результаты диагностики"

        val metricsTable = new TableView[Metrics](metrics) {
          columns ++= Seq(
            new TableColumn[Metrics, String] {
              text = "Snapshot Time"
              cellValueFactory = _.value.snapshotTime
            },
            new TableColumn[Metrics, String]() {
              text = "Node Id"
              cellValueFactory = _.value.nodeId
            },
            new TableColumn[Metrics, String]() {
              text = "Cpu Per Cluster"
              cellValueFactory = _.value.cpuPerCluster
            },
            new TableColumn[Metrics, String]() {
              text = "Response Delay Per Cluster"
              cellValueFactory = _.value.responseDelayCluster
            },
            new TableColumn[Metrics, String]() {
              text = "Response Delay Per Node"
              cellValueFactory = _.value.responseDelayNode
            },
            new TableColumn[Metrics, String]() {
              text = "Status Id"
              cellValueFactory = _.value.statusId
            }
          )
        }

    val diagnosisTable = new TableView[DiagnosticResult](diagnosisResults) {
      columns ++= Seq(
        new TableColumn[DiagnosticResult, String] {
          text = "State"
          cellValueFactory = _.value.state
        },
        new TableColumn[DiagnosticResult, String]() {
          text = "Max P"
          cellValueFactory = _.value.maxP
        },
        new TableColumn[DiagnosticResult, String]() {
          text = "Min P"
          cellValueFactory = _.value.minP
        },
        new TableColumn[DiagnosticResult, String]() {
          text = "Apriori P"
          cellValueFactory = _.value.aprioriP
        }
      )
    }

      val  hbox = new HBox(metricsTable, diagnosisTable)

    hbox.spacing = 20


    stage = new PrimaryStage(){
      title
        scene = new Scene(hbox)
    }

  }
}
