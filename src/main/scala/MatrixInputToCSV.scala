import scala.swing._
import scala.swing.event._
import java.io._
import javax.swing.SwingUtilities
import java.util.concurrent.CountDownLatch

object MatrixInputToCSV extends App {
  val Con = new Converter
  val Cal = new Calculator
  val Run = new Runner
  val sc = Run.sc
  // 🔒 用来让主线程等待直到窗口关闭
  private val latch = new CountDownLatch(1)

  // ✅ 在 Swing 线程中启动 GUI
  SwingUtilities.invokeLater(new Runnable {
    def run(): Unit = {
      println("✅ 启动 Matrix GUI 窗口...")

      val leftArea = new TextArea {
        text = "Please enter a sparse matrix:"
        rows = 12
        columns = 36
      }

      val rightArea = new TextArea {
        text = "Please enter another matrix or vector:"
        rows = 12
        columns = 36
      }

      val resultArea = new TextArea {
        editable = false
        text = "Results display area"
        rows = 6
        columns = 80
      }

      val saveButton = new Button("calculate")
      val clearButton = new Button("clean")

      val frame = new MainFrame {
        title = "Matrix Input and CSV Save"
        preferredSize = new Dimension(800, 480)

        listenTo(saveButton, clearButton)

        reactions += {
          case ButtonClicked(`saveButton`) =>
            try {
              writeCSV(leftArea.text, new File("left_matrix.csv"))
              writeCSV(rightArea.text, new File("right_matrix.csv"))
              val result = Run.Run("left_matrix.csv", "right_matrix.csv")
              resultArea.text = "The result is: \n" + result
            } catch {
              case e: Exception =>
                resultArea.text = s"save failed: ${e.getMessage}"
            }

          case ButtonClicked(`clearButton`) =>
            leftArea.text = ""
            rightArea.text = ""
            resultArea.text = "result area"
        }

        contents = new BorderPanel {
          layout(new BoxPanel(Orientation.Horizontal) {
            contents += new BoxPanel(Orientation.Vertical) {
              contents += new Label("left matrix")
              contents += new ScrollPane(leftArea)
            }
            contents += Swing.HStrut(10)
            contents += new BoxPanel(Orientation.Vertical) {
              contents += new Label("Right matrix/vector")
              contents += new ScrollPane(rightArea)
            }
          }) = BorderPanel.Position.Center

          layout(new BoxPanel(Orientation.Vertical) {
            contents += new FlowPanel(saveButton, clearButton)
            contents += new ScrollPane(resultArea)
          }) = BorderPanel.Position.South
        }

        // 当用户关闭窗口时，释放锁，允许程序退出
        override def closeOperation(): Unit = {
          println("The window closes and the program ends.")
          latch.countDown()
          super.closeOperation()
        }

        centerOnScreen()
        visible = true
      }

      println("GUI 启动完成！窗口应已显示。")
    }
  })

  // 🔒 阻塞主线程直到窗口关闭
  latch.await()

  // ✅ 程序退出
  println("✅ 应用正常退出。")

  // CSV 写入函数
  private def writeCSV(text: String, file: File): Unit = {
    val bw = new BufferedWriter(new FileWriter(file))
    try {
      val lines = text.split("\\r?\\n").map(_.trim).filter(_.nonEmpty)
        .map(_.replaceAll("[\\s]+", ","))
      lines.foreach { l => bw.write(l); bw.newLine() }
    } finally bw.close()
  }
}


