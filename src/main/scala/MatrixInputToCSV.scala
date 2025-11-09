import scala.swing._
import scala.swing.event._
import java.io._
import javax.swing.SwingUtilities
import java.util.concurrent.CountDownLatch

// [ 修复 ] 移除了 Converter 和 Calculator 的导入，因为它们在这里不被直接使用。
// 只需要 Runner，它在内部管理 Converter, Calculator, 和 SparkContext。

object MatrixInputToCSV extends App {
  // [ 修复 ] 只创建 Runner。
  // Runner 类现在在其内部构造函数中正确地创建了 SC, Converter, 和 Calculator。
  // 我们不需要在 MatrixInputToCSV 这个 object 中创建它们。
  val Run = new Runner

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

              // [ 修复 ] 这一行现在可以正常工作了，因为 Run 是唯一需要的实例
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

          // [ 修复 ] 在关闭前必须停止 SparkContext
          // Runner (Run) 持有 sc，所以我们通过它来停止
          println("Shutting down SparkContext...")
          Run.sc.stop()

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

//import scala.swing._
//import scala.swing.event._
//import java.io._
//import javax.swing.SwingUtilities
//import java.util.concurrent.CountDownLatch
//
//object MatrixInputToCSV extends App {
//  val Con = new Converter
//  val Cal = new Calculator
//  val Run = new Runner
//  val sc = Run.sc
//  // 🔒 用来让主线程等待直到窗口关闭
//  private val latch = new CountDownLatch(1)
//
//  // ✅ 在 Swing 线程中启动 GUI
//  SwingUtilities.invokeLater(new Runnable {
//    def run(): Unit = {
//      println("✅ 启动 Matrix GUI 窗口...")
//
//      val leftArea = new TextArea {
//        text = "Please enter a sparse matrix:"
//        rows = 12
//        columns = 36
//      }
//
//      val rightArea = new TextArea {
//        text = "Please enter another matrix or vector:"
//        rows = 12
//        columns = 36
//      }
//
//      val resultArea = new TextArea {
//        editable = false
//        text = "Results display area"
//        rows = 6
//        columns = 80
//      }
//
//      val saveButton = new Button("calculate")
//      val clearButton = new Button("clean")
//
//      val frame = new MainFrame {
//        title = "Matrix Input and CSV Save"
//        preferredSize = new Dimension(800, 480)
//
//        listenTo(saveButton, clearButton)
//
//        reactions += {
//          case ButtonClicked(`saveButton`) =>
//            try {
//              writeCSV(leftArea.text, new File("left_matrix.csv"))
//              writeCSV(rightArea.text, new File("right_matrix.csv"))
//              val result = Run.Run("left_matrix.csv", "right_matrix.csv")
//              resultArea.text = "The result is: \n" + result
//            } catch {
//              case e: Exception =>
//                resultArea.text = s"save failed: ${e.getMessage}"
//            }
//
//          case ButtonClicked(`clearButton`) =>
//            leftArea.text = ""
//            rightArea.text = ""
//            resultArea.text = "result area"
//        }
//
//        contents = new BorderPanel {
//          layout(new BoxPanel(Orientation.Horizontal) {
//            contents += new BoxPanel(Orientation.Vertical) {
//              contents += new Label("left matrix")
//              contents += new ScrollPane(leftArea)
//            }
//            contents += Swing.HStrut(10)
//            contents += new BoxPanel(Orientation.Vertical) {
//              contents += new Label("Right matrix/vector")
//              contents += new ScrollPane(rightArea)
//            }
//          }) = BorderPanel.Position.Center
//
//          layout(new BoxPanel(Orientation.Vertical) {
//            contents += new FlowPanel(saveButton, clearButton)
//            contents += new ScrollPane(resultArea)
//          }) = BorderPanel.Position.South
//        }
//
//        // 当用户关闭窗口时，释放锁，允许程序退出
//        override def closeOperation(): Unit = {
//          println("The window closes and the program ends.")
//          latch.countDown()
//          super.closeOperation()
//        }
//
//        centerOnScreen()
//        visible = true
//      }
//
//      println("GUI 启动完成！窗口应已显示。")
//    }
//  })
//
//  // 🔒 阻塞主线程直到窗口关闭
//  latch.await()
//
//  // ✅ 程序退出
//  println("✅ 应用正常退出。")
//
//  // CSV 写入函数
//  private def writeCSV(text: String, file: File): Unit = {
//    val bw = new BufferedWriter(new FileWriter(file))
//    try {
//      val lines = text.split("\\r?\\n").map(_.trim).filter(_.nonEmpty)
//        .map(_.replaceAll("[\\s]+", ","))
//      lines.foreach { l => bw.write(l); bw.newLine() }
//    } finally bw.close()
//  }
//}

