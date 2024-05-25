package expert

import scalafx.application.JFXApp3
import scalafx.application.JFXApp3.PrimaryStage
import scalafx.collections.ObservableBuffer
import scalafx.scene.Scene
import scalafx.scene.control.{TableColumn, TableView, TextArea}
import scalafx.scene.control.TableColumn._
import scalafx.scene.layout.HBox

object ExpertSystem
  extends JFXApp3 {

  private val metrics = ObservableBuffer[Metrics]()
  val parser = new CsvReader()
  val records: Seq[Array[String]] = parser.parseRecordsGroup("/Users/andrej/IdeaProjects/sparkSandbox/src/main/resources/cluster/today/metrics.csv", List(0, 1, 2, 3, 4, 5))

  records.foreach {
    record => metrics += new Metrics(record.apply(0), record.apply(1), record.apply(2), record.apply(3), record.apply(4), record.apply(5))
  }

  val diagnosisText = parser.parseSingleRecord("/Users/andrej/IdeaProjects/sparkSandbox/src/main/resources/cluster/result/diagnosis.csv")

  override def start(): Unit = {
    val title = "Данные и результаты диагностики"




        val table = new TableView[Metrics](metrics) {
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

      val text = new TextArea(diagnosisText)
      val  hbox = new HBox(table, text)

    hbox.spacing = 20


    stage = new PrimaryStage(){
      title
        scene = new Scene(hbox)
    }

  }
}
