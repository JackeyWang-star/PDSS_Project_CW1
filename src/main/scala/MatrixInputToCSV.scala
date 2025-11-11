import scala.swing._
import scala.swing.event._
import scala.concurrent._
import ExecutionContext.Implicits.global
import java.io._
import java.awt.{Color, Dimension}
import java.nio.charset.CodingErrorAction
import java.awt.event.{ActionEvent, KeyEvent, InputEvent}

object MatrixInputToCSV extends SimpleSwingApplication {

  // --- 后端 Runner（内部管理 Spark / Converter / Calculator） ---
  val Run = new Runner

  // --- 不可见字符清洗（BOM / 零宽 / 不换行空格 等） ---
  private val InvisibleChars: Array[Char] =
    Array('\uFEFF', '\u200B', '\u2060', '\u00A0', '\u202A', '\u202B', '\u202C', '\u202D', '\u202E')

  private def scrubInvisibles(s: String): String =
    InvisibleChars.foldLeft(s)((acc, ch) => acc.replace(ch.toString, ""))

  // 文本 → 规范 CSV 行数组（去不可见字符、折叠空白为逗号、去空行）
  private def normalizeToCsv(text: String): Array[String] =
    text.split("\\r?\\n")
      .iterator
      .map(scrubInvisibles)
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(_.replaceAll("[\\s]+", ","))
      .toArray

  // 估形状（忽略非数字 token），返回 (rows, cols) 或错误信息
  private def inferShape(lines: Array[String]): Either[String, (Int, Int)] = {
    if (lines.isEmpty) return Left("空输入")
    var rows = 0
    var minC = Int.MaxValue; var maxC = Int.MinValue
    lines.foreach { line =>
      val toks = line.split(",").map(_.trim).filter(_.nonEmpty)
      val numeric = toks.flatMap(t => util.Try(t.toDouble).toOption)
      if (numeric.nonEmpty) {
        rows += 1
        val c = numeric.length
        if (c < minC) minC = c
        if (c > maxC) maxC = c
      }
    }
    if (rows == 0) Left("没有有效的数字行")
    else if (minC != maxC) Left(s"不矩形：最小列数=$minC, 最大列数=$maxC")
    else Right((rows, minC))
  }

  // 保存 CSV 行到文件
  private def writeCSV(lines: Array[String], target: File): Unit = {
    val bw = new BufferedWriter(new FileWriter(target))
    try lines.foreach { l => bw.write(l); bw.newLine() }
    finally bw.close()
  }

  // 读文件到文本（逐行清洗不可见字符）
  private def readFileToText(f: File): String = {
    val codec = scala.io.Codec.UTF8
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    val src = scala.io.Source.fromFile(f)(codec)
    try src.getLines().map(scrubInvisibles).mkString("\n")
    finally src.close()
  }

  // ------------------- UI 组件 -------------------
  val leftArea = new TextArea {
    text = "4,0,9,0,0\n0,7,0,0,0\n0,0,0,0,0\n0,0,0,5,0"
    rows = 12; columns = 36
  }
  val rightArea = new TextArea {
    text = "1,0,0,0\n0,0,4,1\n2,0,0,0\n3,0,0,1\n0,0,2,0"
    rows = 12; columns = 36
  }
  val resultArea = new TextArea {
    editable = false; rows = 10; columns = 90
    text = "结果展示区"
  }

  // 状态标签
  val leftShape    = new Label("左形状：-")
  val rightShape   = new Label("右形状：-")
  val dimCheck     = new Label("维度检查：-")
  val statusLabel  = new Label("状态：就绪")
  val timeLabel    = new Label("耗时：-")

  // 控件区
  val runBtn    = new Button("计算")
  val clearBtn  = new Button("清空")
  val loadLeft  = new Button("载入左矩阵…")
  val loadRight = new Button("载入右矩阵/向量…")
  val saveInput = new Button("保存输入…")
  val saveRes   = new Button("保存结果…")

  // 选项（仅影响预览展示，不影响后端计算）
  val previewRowsField = new TextField("20", 5)
  val previewColsField = new TextField("120", 5)
  val orderedOutCheck  = new CheckBox("输出按行排序（默认）") { selected = true }

  val progress = new ProgressBar {
    min = 0; max = 100; value = 0; indeterminate = false
    preferredSize = new Dimension(180, 16)
  }

  // ------------------- 逻辑 -------------------
  private def refreshMeta(): Unit = {
    val leftCsv  = normalizeToCsv(leftArea.text)
    val rightCsv = normalizeToCsv(rightArea.text)

    val L = inferShape(leftCsv)
    val R = inferShape(rightCsv)

    def ok(l: Label): Unit = l.foreground = new Color(0,128,0)
    def bad(l: Label): Unit = l.foreground = Color.RED

    L match {
      case Right((r,c)) => leftShape.text = s"左形状：$r x $c"; ok(leftShape)
      case Left(msg)    => leftShape.text = s"左形状：$msg";    bad(leftShape)
    }
    R match {
      case Right((r,c)) => rightShape.text = s"右形状：$r x $c"; ok(rightShape)
      case Left(msg)    => rightShape.text = s"右形状：$msg";    bad(rightShape)
    }

    (L, R) match {
      case (Right((_, ac)), Right((br, _))) =>
        val pass = ac == br
        dimCheck.text = if (pass) s"维度检查：OK（A.cols=$ac == B.rows=$br）"
        else s"维度检查：不匹配（A.cols=$ac ≠ B.rows=$br）"
        if (pass) ok(dimCheck) else bad(dimCheck)
      case _ =>
        dimCheck.text = "维度检查：-"
        dimCheck.foreground = Color.DARK_GRAY
    }
  }

  private def setBusy(b: Boolean): Unit = {
    runBtn.enabled = !b; clearBtn.enabled = !b
    loadLeft.enabled = !b; loadRight.enabled = !b
    saveInput.enabled = !b; saveRes.enabled = !b
    progress.indeterminate = b
    statusLabel.text = if (b) "状态：计算中…" else "状态：就绪"
  }

  private def chooseFile(toSave: Boolean, title: String): Option[File] = {
    val fc = new FileChooser(new File("."))
    fc.title = title
    fc.fileSelectionMode = FileChooser.SelectionMode.FilesOnly
    val res = if (toSave) fc.showSaveDialog(null) else fc.showOpenDialog(null)
    if (res == FileChooser.Result.Approve) Option(fc.selectedFile) else None
  }

  private def saveResultToFile(): Unit = {
    val payloadOpt: Option[String] = resultArea.peer.getClientProperty("FULL_RESULT_STRING") match {
      case s: String => Some(s)
      case _         => None
    }
    payloadOpt match {
      case None =>
        Dialog.showMessage(null, "没有可保存的结果（请先计算）。", "提示", Dialog.Message.Info)
      case Some(all) =>
        chooseFile(toSave = true, "保存结果到…").foreach { f =>
          val target = if (f.isDirectory) new File(f, "result.csv") else f
          val bw = new BufferedWriter(new FileWriter(target))
          try bw.write(all) finally bw.close()
          Dialog.showMessage(null, s"已保存到：${target.getAbsolutePath}", "成功", Dialog.Message.Info)
        }
    }
  }

  private def saveInputToFiles(): Unit = {
    chooseFile(toSave = true, "保存当前输入为 CSV（目录将包含 left/right）…").foreach { f =>
      val base = if (f.isDirectory) f else f.getParentFile
      val leftCsv  = normalizeToCsv(leftArea.text)
      val rightCsv = normalizeToCsv(rightArea.text)
      writeCSV(leftCsv,  new File(base, "left_matrix.csv"))
      writeCSV(rightCsv, new File(base, "right_matrix.csv"))
      Dialog.showMessage(null, s"输入已保存到：${base.getAbsolutePath}", "成功", Dialog.Message.Info)
    }
  }

  private def runOnce(): Unit = {
    val leftCsv  = normalizeToCsv(leftArea.text)
    val rightCsv = normalizeToCsv(rightArea.text)

    (inferShape(leftCsv), inferShape(rightCsv)) match {
      case (Right((_, ac)), Right((br, _))) if ac == br => // ok
      case (Right((_, ac)), Right((br, _))) =>
        Dialog.showMessage(null, s"维度不匹配：A.cols=$ac, B.rows=$br", "错误", Dialog.Message.Error); return
      case (Left(m), _) =>
        Dialog.showMessage(null, s"左侧输入错误：$m", "错误", Dialog.Message.Error); return
      case (_, Left(m)) =>
        Dialog.showMessage(null, s"右侧输入错误：$m", "错误", Dialog.Message.Error); return
    }

    setBusy(true); timeLabel.text = "耗时：-"; progress.value = 0
    val t0 = System.nanoTime()

    Future {
      val leftFile  = File.createTempFile("left_matrix_", ".csv")
      val rightFile = File.createTempFile("right_matrix_", ".csv")
      leftFile.deleteOnExit(); rightFile.deleteOnExit()
      writeCSV(leftCsv, leftFile)
      writeCSV(rightCsv, rightFile)

      try {
        val resultString = Run.Run(leftFile.getAbsolutePath, rightFile.getAbsolutePath)
        val nRows  = util.Try(previewRowsField.text.trim.toInt).getOrElse(20).max(1)
        val nColsC = util.Try(previewColsField.text.trim.toInt).getOrElse(120).max(20)
        val preview = resultString.split("\\r?\\n").take(nRows).map { line =>
          if (line.length <= nColsC) line else line.take(nColsC) + " …"
        }.mkString("\n")
        (resultString, preview)
      } finally {
        leftFile.delete(); rightFile.delete()
      }
    }.map { case (resultString, preview) =>
      val ms = (System.nanoTime() - t0) / 1e6
      resultArea.text = preview
      timeLabel.text  = f"耗时：$ms%.1f ms"
      statusLabel.text = s"状态：完成（预览前 ${preview.split('\n').length} 行）"
      resultArea.peer.putClientProperty("FULL_RESULT_STRING", resultString)
      setBusy(false)
    }.recover { case e =>
      resultArea.text = s"计算失败：${e.getMessage}"
      statusLabel.text = "状态：失败"
      setBusy(false)
    }
  }

  // ------------------- 顶层 UI -------------------
  def top: MainFrame = new MainFrame {
    title = "PDSS CW1 — Matrix Frontend (Pure RDD Runtime)"
    preferredSize = new Dimension(1100, 700)

    // 菜单栏：保证“保存结果/保存输入”不丢
    menuBar = new MenuBar {
      contents += new Menu("文件") {
        contents += new MenuItem(Action("保存结果…")(saveResultToFile()))
        contents += new MenuItem(Action("保存输入…")(saveInputToFiles()))
        contents += new Separator
        contents += new MenuItem(Action("退出"){ quit() })
      }
      contents += new Menu("帮助") {
        contents += new MenuItem(Action("关于"){
          Dialog.showMessage(null, "Matrix Frontend for PDSS CW1\nPure RDD Runtime\n© 2025", "关于", Dialog.Message.Info)
        })
      }
    }

    contents = new BorderPanel {
      layout(new BoxPanel(Orientation.Horizontal) {
        contents += new BoxPanel(Orientation.Vertical) {
          contents += new Label("Left Matrix (A)")
          contents += new ScrollPane(leftArea) { preferredSize = new Dimension(500, 260) }
        }
        contents += Swing.HStrut(10)
        contents += new BoxPanel(Orientation.Vertical) {
          contents += new Label("Right Matrix / Vector (B / x)")
          contents += new ScrollPane(rightArea) { preferredSize = new Dimension(500, 260) }
        }
      }) = BorderPanel.Position.Center

      layout(new BoxPanel(Orientation.Vertical) {
        contents += new FlowPanel(
          new Label("预览前 N 行："), previewRowsField,
          new Label("每行最多 N 字符："), previewColsField,
          orderedOutCheck,
          Swing.HStrut(16),
          runBtn, clearBtn, loadLeft, loadRight, saveInput, saveRes,
          Swing.HStrut(16), progress
        )
        contents += new FlowPanel(leftShape, Swing.HStrut(20), rightShape, Swing.HStrut(20),
          dimCheck, Swing.HStrut(20), timeLabel, Swing.HStrut(20), statusLabel)
        contents += new ScrollPane(resultArea) { preferredSize = new Dimension(1060, 260) }
      }) = BorderPanel.Position.South
    }

    // 事件绑定
    listenTo(runBtn, clearBtn, loadLeft, loadRight, saveInput, saveRes)
    reactions += {
      case ButtonClicked(`runBtn`)    => refreshMeta(); runOnce()
      case ButtonClicked(`clearBtn`)  =>
        leftArea.text = ""; rightArea.text = ""; resultArea.text = "结果展示区"
        leftShape.text = "左形状：-"; rightShape.text = "右形状：-"
        dimCheck.text = "维度检查：-"; dimCheck.foreground = Color.DARK_GRAY
        timeLabel.text = "耗时：-"; statusLabel.text = "状态：就绪"
      case ButtonClicked(`loadLeft`)  =>
        chooseFile(toSave = false, "选择左矩阵 CSV…").foreach { f =>
          leftArea.text = readFileToText(f); refreshMeta()
        }
      case ButtonClicked(`loadRight`) =>
        chooseFile(toSave = false, "选择右矩阵/向量 CSV…").foreach { f =>
          rightArea.text = readFileToText(f); refreshMeta()
        }
      case ButtonClicked(`saveInput`) => saveInputToFiles()
      case ButtonClicked(`saveRes`)   => saveResultToFile()
    }

    // 快捷键：Ctrl+S 保存结果
    peer.getRootPane.registerKeyboardAction(
      (_: ActionEvent) => saveResultToFile(),
      javax.swing.KeyStroke.getKeyStroke(KeyEvent.VK_S, InputEvent.CTRL_DOWN_MASK),
      javax.swing.JComponent.WHEN_IN_FOCUSED_WINDOW
    )

    // 初始刷新形状提示
    refreshMeta()

    // 关闭窗口 → 停 Spark（防资源泄露）
    override def closeOperation(): Unit = {
      try { println("Shutting down SparkContext..."); Run.sc.stop() }
      finally { super.closeOperation() }
    }

    centerOnScreen()
  }
}
