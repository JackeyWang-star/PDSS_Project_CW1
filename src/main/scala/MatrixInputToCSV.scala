//import scala.swing._
//import scala.swing.event._
//import scala.concurrent._
//import ExecutionContext.Implicits.global
//import java.io._
//import java.awt.{Color, Dimension}
//import java.nio.charset.CodingErrorAction
//import java.awt.event.{ActionEvent, KeyEvent, InputEvent}
//
//object MatrixInputToCSV extends SimpleSwingApplication {
//
//  // --- 后端 Runner（内部管理 Spark / Converter / Calculator） ---
//  val Run = new Runner
//
//  // --- 不可见字符清洗（BOM / 零宽 / 不换行空格 等） ---
//  private val InvisibleChars: Array[Char] =
//    Array('\uFEFF', '\u200B', '\u2060', '\u00A0', '\u202A', '\u202B', '\u202C', '\u202D', '\u202E')
//
//  private def scrubInvisibles(s: String): String =
//    InvisibleChars.foldLeft(s)((acc, ch) => acc.replace(ch.toString, ""))
//
//  // 文本 → 规范 CSV 行数组（去不可见字符、折叠空白为逗号、去空行）
//  private def normalizeToCsv(text: String): Array[String] =
//    text.split("\\r?\\n")
//      .iterator
//      .map(scrubInvisibles)
//      .map(_.trim)
//      .filter(_.nonEmpty)
//      .map(_.replaceAll("[\\s]+", ","))
//      .toArray
//
//  // 估形状（忽略非数字 token），返回 (rows, cols) 或错误信息
//  private def inferShape(lines: Array[String]): Either[String, (Int, Int)] = {
//    if (lines.isEmpty) return Left("空输入")
//    var rows = 0
//    var minC = Int.MaxValue; var maxC = Int.MinValue
//    lines.foreach { line =>
//      val toks = line.split(",").map(_.trim).filter(_.nonEmpty)
//      val numeric = toks.flatMap(t => util.Try(t.toDouble).toOption)
//      if (numeric.nonEmpty) {
//        rows += 1
//        val c = numeric.length
//        if (c < minC) minC = c
//        if (c > maxC) maxC = c
//      }
//    }
//    if (rows == 0) Left("没有有效的数字行")
//    else if (minC != maxC) Left(s"不矩形：最小列数=$minC, 最大列数=$maxC")
//    else Right((rows, minC))
//  }
//
//  // 保存 CSV 行到文件
//  private def writeCSV(lines: Array[String], target: File): Unit = {
//    val bw = new BufferedWriter(new FileWriter(target))
//    try lines.foreach { l => bw.write(l); bw.newLine() }
//    finally bw.close()
//  }
//
//  // 读文件到文本（逐行清洗不可见字符）
//  private def readFileToText(f: File): String = {
//    val codec = scala.io.Codec.UTF8
//    codec.onMalformedInput(CodingErrorAction.REPLACE)
//    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
//    val src = scala.io.Source.fromFile(f)(codec)
//    try src.getLines().map(scrubInvisibles).mkString("\n")
//    finally src.close()
//  }
//
//  // ------------------- UI 组件 -------------------
//  val leftArea = new TextArea {
//    text = "4,0,9,0,0\n0,7,0,0,0\n0,0,0,0,0\n0,0,0,5,0"
//    rows = 12; columns = 36
//  }
//  val rightArea = new TextArea {
//    text = "1,0,0,0\n0,0,4,1\n2,0,0,0\n3,0,0,1\n0,0,2,0"
//    rows = 12; columns = 36
//  }
//  val resultArea = new TextArea {
//    editable = false; rows = 10; columns = 90
//    text = "结果展示区"
//  }
//
//  // 状态标签
//  val leftShape    = new Label("左形状：-")
//  val rightShape   = new Label("右形状：-")
//  val dimCheck     = new Label("维度检查：-")
//  val statusLabel  = new Label("状态：就绪")
//  val timeLabel    = new Label("耗时：-")
//
//  // 控件区
//  val runBtn    = new Button("计算")
//  val clearBtn  = new Button("清空")
//  val loadLeft  = new Button("载入左矩阵…")
//  val loadRight = new Button("载入右矩阵/向量…")
//  val saveInput = new Button("保存输入…")
//  val saveRes   = new Button("保存结果…")
//
//  // 选项（仅影响预览展示，不影响后端计算）
//  val previewRowsField = new TextField("20", 5)
//  val previewColsField = new TextField("120", 5)
//  val orderedOutCheck  = new CheckBox("输出按行排序（默认）") { selected = true }
//
//  val progress = new ProgressBar {
//    min = 0; max = 100; value = 0; indeterminate = false
//    preferredSize = new Dimension(180, 16)
//  }
//
//  // ------------------- 逻辑 -------------------
//  private def refreshMeta(): Unit = {
//    val leftCsv  = normalizeToCsv(leftArea.text)
//    val rightCsv = normalizeToCsv(rightArea.text)
//
//    val L = inferShape(leftCsv)
//    val R = inferShape(rightCsv)
//
//    def ok(l: Label): Unit = l.foreground = new Color(0,128,0)
//    def bad(l: Label): Unit = l.foreground = Color.RED
//
//    L match {
//      case Right((r,c)) => leftShape.text = s"左形状：$r x $c"; ok(leftShape)
//      case Left(msg)    => leftShape.text = s"左形状：$msg";    bad(leftShape)
//    }
//    R match {
//      case Right((r,c)) => rightShape.text = s"右形状：$r x $c"; ok(rightShape)
//      case Left(msg)    => rightShape.text = s"右形状：$msg";    bad(rightShape)
//    }
//
//    (L, R) match {
//      case (Right((_, ac)), Right((br, _))) =>
//        val pass = ac == br
//        dimCheck.text = if (pass) s"维度检查：OK（A.cols=$ac == B.rows=$br）"
//        else s"维度检查：不匹配（A.cols=$ac ≠ B.rows=$br）"
//        if (pass) ok(dimCheck) else bad(dimCheck)
//      case _ =>
//        dimCheck.text = "维度检查：-"
//        dimCheck.foreground = Color.DARK_GRAY
//    }
//  }
//
//  private def setBusy(b: Boolean): Unit = {
//    runBtn.enabled = !b; clearBtn.enabled = !b
//    loadLeft.enabled = !b; loadRight.enabled = !b
//    saveInput.enabled = !b; saveRes.enabled = !b
//    progress.indeterminate = b
//    statusLabel.text = if (b) "状态：计算中…" else "状态：就绪"
//  }
//
//  private def chooseFile(toSave: Boolean, title: String): Option[File] = {
//    val fc = new FileChooser(new File("."))
//    fc.title = title
//    fc.fileSelectionMode = FileChooser.SelectionMode.FilesOnly
//    val res = if (toSave) fc.showSaveDialog(null) else fc.showOpenDialog(null)
//    if (res == FileChooser.Result.Approve) Option(fc.selectedFile) else None
//  }
//
//  private def saveResultToFile(): Unit = {
//    val payloadOpt: Option[String] = resultArea.peer.getClientProperty("FULL_RESULT_STRING") match {
//      case s: String => Some(s)
//      case _         => None
//    }
//    payloadOpt match {
//      case None =>
//        Dialog.showMessage(null, "没有可保存的结果（请先计算）。", "提示", Dialog.Message.Info)
//      case Some(all) =>
//        chooseFile(toSave = true, "保存结果到…").foreach { f =>
//          val target = if (f.isDirectory) new File(f, "result.csv") else f
//          val bw = new BufferedWriter(new FileWriter(target))
//          try bw.write(all) finally bw.close()
//          Dialog.showMessage(null, s"已保存到：${target.getAbsolutePath}", "成功", Dialog.Message.Info)
//        }
//    }
//  }
//
//  private def saveInputToFiles(): Unit = {
//    chooseFile(toSave = true, "保存当前输入为 CSV（目录将包含 left/right）…").foreach { f =>
//      val base = if (f.isDirectory) f else f.getParentFile
//      val leftCsv  = normalizeToCsv(leftArea.text)
//      val rightCsv = normalizeToCsv(rightArea.text)
//      writeCSV(leftCsv,  new File(base, "left_matrix.csv"))
//      writeCSV(rightCsv, new File(base, "right_matrix.csv"))
//      Dialog.showMessage(null, s"输入已保存到：${base.getAbsolutePath}", "成功", Dialog.Message.Info)
//    }
//  }
//
//  private def runOnce(): Unit = {
//    val leftCsv  = normalizeToCsv(leftArea.text)
//    val rightCsv = normalizeToCsv(rightArea.text)
//
//    (inferShape(leftCsv), inferShape(rightCsv)) match {
//      case (Right((_, ac)), Right((br, _))) if ac == br => // ok
//      case (Right((_, ac)), Right((br, _))) =>
//        Dialog.showMessage(null, s"维度不匹配：A.cols=$ac, B.rows=$br", "错误", Dialog.Message.Error); return
//      case (Left(m), _) =>
//        Dialog.showMessage(null, s"左侧输入错误：$m", "错误", Dialog.Message.Error); return
//      case (_, Left(m)) =>
//        Dialog.showMessage(null, s"右侧输入错误：$m", "错误", Dialog.Message.Error); return
//    }
//
//    setBusy(true); timeLabel.text = "耗时：-"; progress.value = 0
//    val t0 = System.nanoTime()
//
//    Future {
//      val leftFile  = File.createTempFile("left_matrix_", ".csv")
//      val rightFile = File.createTempFile("right_matrix_", ".csv")
//      leftFile.deleteOnExit(); rightFile.deleteOnExit()
//      writeCSV(leftCsv, leftFile)
//      writeCSV(rightCsv, rightFile)
//
//      try {
//        val resultString = Run.Run(leftFile.getAbsolutePath, rightFile.getAbsolutePath)
//        val nRows  = util.Try(previewRowsField.text.trim.toInt).getOrElse(20).max(1)
//        val nColsC = util.Try(previewColsField.text.trim.toInt).getOrElse(120).max(20)
//        val preview = resultString.split("\\r?\\n").take(nRows).map { line =>
//          if (line.length <= nColsC) line else line.take(nColsC) + " …"
//        }.mkString("\n")
//        (resultString, preview)
//      } finally {
//        leftFile.delete(); rightFile.delete()
//      }
//    }.map { case (resultString, preview) =>
//      val ms = (System.nanoTime() - t0) / 1e6
//      resultArea.text = preview
//      timeLabel.text  = f"耗时：$ms%.1f ms"
//      statusLabel.text = s"状态：完成（预览前 ${preview.split('\n').length} 行）"
//      resultArea.peer.putClientProperty("FULL_RESULT_STRING", resultString)
//      setBusy(false)
//    }.recover { case e =>
//      resultArea.text = s"计算失败：${e.getMessage}"
//      statusLabel.text = "状态：失败"
//      setBusy(false)
//    }
//  }
//
//  // ------------------- 顶层 UI -------------------
//  def top: MainFrame = new MainFrame {
//    title = "PDSS CW1 — Matrix Frontend (Pure RDD Runtime)"
//    preferredSize = new Dimension(1100, 700)
//
//    // 菜单栏：保证“保存结果/保存输入”不丢
//    menuBar = new MenuBar {
//      contents += new Menu("文件") {
//        contents += new MenuItem(Action("保存结果…")(saveResultToFile()))
//        contents += new MenuItem(Action("保存输入…")(saveInputToFiles()))
//        contents += new Separator
//        contents += new MenuItem(Action("退出"){ quit() })
//      }
//      contents += new Menu("帮助") {
//        contents += new MenuItem(Action("关于"){
//          Dialog.showMessage(null, "Matrix Frontend for PDSS CW1\nPure RDD Runtime\n© 2025", "关于", Dialog.Message.Info)
//        })
//      }
//    }
//
//    contents = new BorderPanel {
//      layout(new BoxPanel(Orientation.Horizontal) {
//        contents += new BoxPanel(Orientation.Vertical) {
//          contents += new Label("Left Matrix (A)")
//          contents += new ScrollPane(leftArea) { preferredSize = new Dimension(500, 260) }
//        }
//        contents += Swing.HStrut(10)
//        contents += new BoxPanel(Orientation.Vertical) {
//          contents += new Label("Right Matrix / Vector (B / x)")
//          contents += new ScrollPane(rightArea) { preferredSize = new Dimension(500, 260) }
//        }
//      }) = BorderPanel.Position.Center
//
//      layout(new BoxPanel(Orientation.Vertical) {
//        contents += new FlowPanel(
//          new Label("预览前 N 行："), previewRowsField,
//          new Label("每行最多 N 字符："), previewColsField,
//          orderedOutCheck,
//          Swing.HStrut(16),
//          runBtn, clearBtn, loadLeft, loadRight, saveInput, saveRes,
//          Swing.HStrut(16), progress
//        )
//        contents += new FlowPanel(leftShape, Swing.HStrut(20), rightShape, Swing.HStrut(20),
//          dimCheck, Swing.HStrut(20), timeLabel, Swing.HStrut(20), statusLabel)
//        contents += new ScrollPane(resultArea) { preferredSize = new Dimension(1060, 260) }
//      }) = BorderPanel.Position.South
//    }
//
//    // 事件绑定
//    listenTo(runBtn, clearBtn, loadLeft, loadRight, saveInput, saveRes)
//    reactions += {
//      case ButtonClicked(`runBtn`)    => refreshMeta(); runOnce()
//      case ButtonClicked(`clearBtn`)  =>
//        leftArea.text = ""; rightArea.text = ""; resultArea.text = "结果展示区"
//        leftShape.text = "左形状：-"; rightShape.text = "右形状：-"
//        dimCheck.text = "维度检查：-"; dimCheck.foreground = Color.DARK_GRAY
//        timeLabel.text = "耗时：-"; statusLabel.text = "状态：就绪"
//      case ButtonClicked(`loadLeft`)  =>
//        chooseFile(toSave = false, "选择左矩阵 CSV…").foreach { f =>
//          leftArea.text = readFileToText(f); refreshMeta()
//        }
//      case ButtonClicked(`loadRight`) =>
//        chooseFile(toSave = false, "选择右矩阵/向量 CSV…").foreach { f =>
//          rightArea.text = readFileToText(f); refreshMeta()
//        }
//      case ButtonClicked(`saveInput`) => saveInputToFiles()
//      case ButtonClicked(`saveRes`)   => saveResultToFile()
//    }
//
//    // 快捷键：Ctrl+S 保存结果
//    peer.getRootPane.registerKeyboardAction(
//      (_: ActionEvent) => saveResultToFile(),
//      javax.swing.KeyStroke.getKeyStroke(KeyEvent.VK_S, InputEvent.CTRL_DOWN_MASK),
//      javax.swing.JComponent.WHEN_IN_FOCUSED_WINDOW
//    )
//
//    // 初始刷新形状提示
//    refreshMeta()
//
//    // 关闭窗口 → 停 Spark（防资源泄露）
//    override def closeOperation(): Unit = {
//      try { println("Shutting down SparkContext..."); Run.sc.stop() }
//      finally { super.closeOperation() }
//    }
//
//    centerOnScreen()
//  }
//}

import scala.swing._
import scala.swing.event._
import scala.concurrent._
import ExecutionContext.Implicits.global
import java.io._
import java.awt.{Color, Dimension}
import java.nio.charset.CodingErrorAction
import java.awt.event.{ActionEvent, KeyEvent, InputEvent}

// Spark 指标采集
import org.apache.spark.SparkContext
import org.apache.spark.scheduler._
import org.apache.spark.sql.{SparkSession, Dataset, DataFrame}
import org.apache.spark.sql.functions._
import scala.util.Try

object MatrixInputToCSV extends SimpleSwingApplication {

  // ============ 后端 Runner（内部管理 Spark / Converter / Calculator） ============
  val Run = new Runner

  // -------- DataFrame 基线使用的 case class --------
  case class Entry(i: Int, j: Int, v: Double)
  case class Vec(k: Int, x: Double)

  // ============ Spark 指标采集器：每次计算前 reset，后 snapshot ============
  object MetricsCollector extends SparkListener {
    case class Snapshot(
                         label: String,
                         jobs: Int, stages: Int, tasks: Int,
                         shuffleRead: Long, shuffleWrite: Long,
                         inputBytes: Long, outputBytes: Long,
                         memSpill: Long, diskSpill: Long,
                         gcTimeMs: Long,
                         taskTimesMs: Vector[Long],
                         wallMs: Double
                       )
    private var installed = false
    private var jobs = 0
    private var stages = 0
    private var tasks = 0
    private var shuffleRead = 0L
    private var shuffleWrite = 0L
    private var inputBytes = 0L
    private var outputBytes = 0L
    private var memSpill = 0L
    private var diskSpill = 0L
    private var gcTime = 0L
    private var taskTimes = Vector.empty[Long]

    def install(sc: SparkContext): Unit = synchronized {
      if (!installed) { sc.addSparkListener(this); installed = true }
    }
    def reset(): Unit = synchronized {
      jobs = 0; stages = 0; tasks = 0
      shuffleRead = 0L; shuffleWrite = 0L
      inputBytes = 0L; outputBytes = 0L
      memSpill = 0L; diskSpill = 0L
      gcTime = 0L
      taskTimes = Vector.empty
    }
    def snapshot(label: String, wallMs: Double): Snapshot = synchronized {
      Snapshot(label, jobs, stages, tasks, shuffleRead, shuffleWrite,
        inputBytes, outputBytes, memSpill, diskSpill, gcTime, taskTimes, wallMs)
    }
    override def onJobStart(e: SparkListenerJobStart): Unit = synchronized { jobs += 1 }
    override def onStageCompleted(e: SparkListenerStageCompleted): Unit = synchronized { stages += 1 }
    override def onTaskEnd(e: SparkListenerTaskEnd): Unit = synchronized {
      val m = e.taskMetrics
      tasks += 1
      if (m != null) {
        taskTimes :+= m.executorRunTime
        gcTime += m.jvmGCTime
        if (m.inputMetrics != null) inputBytes += m.inputMetrics.bytesRead
        if (m.outputMetrics != null) outputBytes += m.outputMetrics.bytesWritten
        if (m.shuffleReadMetrics != null) shuffleRead += m.shuffleReadMetrics.totalBytesRead
        if (m.shuffleWriteMetrics != null) shuffleWrite += m.shuffleWriteMetrics.bytesWritten
        memSpill += m.memoryBytesSpilled
        diskSpill += m.diskBytesSpilled
      }
    }
  }

  // ============ 不可见字符清洗（BOM/零宽/不换行空格） ============
  private val InvisibleChars: Array[Char] =
    Array('\uFEFF', '\u200B', '\u2060', '\u00A0', '\u202A', '\u202B', '\u202C', '\u202D', '\u202E')
  private def scrubInvisibles(s: String): String =
    InvisibleChars.foldLeft(s)((acc, ch) => acc.replace(ch.toString, ""))

  // 文本 → 规范 CSV 行数组
  private def normalizeToCsv(text: String): Array[String] =
    text.split("\\r?\\n").iterator
      .map(scrubInvisibles).map(_.trim).filter(_.nonEmpty)
      .map(_.replaceAll("[\\s]+", ",")).toArray

  // 估形状（忽略非数字 token）
  private def inferShape(lines: Array[String]): Either[String, (Int, Int)] = {
    if (lines.isEmpty) return Left("空输入")
    var rows = 0; var minC = Int.MaxValue; var maxC = Int.MinValue
    lines.foreach { line =>
      val toks = line.split(",").map(_.trim).filter(_.nonEmpty)
      val numeric = toks.flatMap(s => Try(s.toDouble).toOption)
      if (numeric.nonEmpty) {
        rows += 1; val c = numeric.length
        if (c < minC) minC = c; if (c > maxC) maxC = c
      }
    }
    if (rows == 0) Left("没有有效的数字行")
    else if (minC != maxC) Left(s"不矩形：最小列数=$minC, 最大列数=$maxC")
    else Right((rows, minC))
  }

  private def writeCSV(lines: Array[String], target: File): Unit = {
    val bw = new BufferedWriter(new FileWriter(target))
    try lines.foreach { l => bw.write(l); bw.newLine() } finally bw.close()
  }
  private def readFileToText(f: File): String = {
    val codec = scala.io.Codec.UTF8
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    val src = scala.io.Source.fromFile(f)(codec)
    try src.getLines().map(scrubInvisibles).mkString("\n") finally src.close()
  }

  // ============ DataFrame 基线：读/算/取字符串 ============
  private def spark(): SparkSession =
    SparkSession.builder().config(Run.sc.getConf).getOrCreate()

  private def dfReadDenseCsvAsCOO(path: String): Dataset[Entry] = {
    val sp = spark(); import sp.implicits._
    val lines = Run.sc.textFile(path)
    val coo = lines.zipWithIndex().flatMap { case (line, rowIdx) =>
      val toks = line.split(",").map(_.trim).filter(_.nonEmpty)
      toks.zipWithIndex.flatMap { case (s, colIdx) =>
        Try(s.toDouble).toOption match {
          case Some(d) if d != 0.0 => Some(Entry(rowIdx.toInt, colIdx, d))
          case _ => None
        }
      }
    }
    sp.createDataset(coo)
  }
  private def dfReadVector(path: String): Dataset[Vec] = {
    val sp = spark(); import sp.implicits._
    val first = Run.sc.textFile(path).filter(_.trim.nonEmpty).first()
    val toks  = first.split(",").map(_.trim).filter(_.nonEmpty)
    val vec   = toks.zipWithIndex.flatMap { case (s, idx) => Try(s.toDouble).toOption.map(d => Vec(idx, d)) }
    sp.createDataset(vec.toSeq)
  }
  private def dfSpmvToString(pathA: String, pathX: String): String = {
    val sp = spark(); import sp.implicits._
    val A = dfReadDenseCsvAsCOO(pathA)
    val x = dfReadVector(pathX)
    val Aj = A.withColumnRenamed("j","k").withColumnRenamed("v","aik")
    val X  = x.toDF("k","x")
    val dfY = Aj.join(X, Seq("k"))
      .select(col("i"), (col("aik")*col("x")).as("prod"))
      .groupBy("i").agg(sum("prod").as("y"))
      .orderBy("i")
      .select(col("y"))
    // 合成一行字符串（与 RDD 预览方式一致）
    val onePart = dfY.rdd.map(_.getDouble(0).toString)
      .coalesce(1, shuffle = true).mapPartitions(it => Iterator.single(it.mkString(",")))
    onePart.first()
  }
  // DataFrame: A·B → 稠密矩阵字符串（与 RDD 预览一致）
  private def dfSpmmToDenseString(pathA: String, pathB: String, aRows: Int, bCols: Int): String = {
    val sp = spark(); import sp.implicits._
    val A = dfReadDenseCsvAsCOO(pathA)   // Dataset[Entry(i,j,v)]
    val B = dfReadDenseCsvAsCOO(pathB)

    // DF 做乘法并 group 成 COO(i,j,cij)
    val Ak = A.withColumnRenamed("j","k").withColumnRenamed("v","aik")
    val Bk = B.withColumnRenamed("i","k").withColumnRenamed("v","kbj")
    val dfC = Ak.join(Bk, Seq("k"))
      .select(org.apache.spark.sql.functions.col("i"),
        org.apache.spark.sql.functions.col("j"),
        (org.apache.spark.sql.functions.col("aik") * org.apache.spark.sql.functions.col("kbj")).as("prod"))
      .groupBy("i","j").agg(org.apache.spark.sql.functions.sum("prod").as("cij"))

    // 用 RDD 管道把 DF 的 COO 稠密化成逐行 CSV（与 RDD 侧完全一致）
    val sc = Run.sc
    val P  = sc.defaultParallelism
    val part = new org.apache.spark.HashPartitioner(P)

    val rows: org.apache.spark.rdd.RDD[(Int,(Int,Double))] =
      dfC.rdd.map(r => (r.getInt(0), (r.getInt(1), r.getDouble(2))))

    val rowMap = rows.partitionBy(part).combineByKey[scala.collection.mutable.Map[Int,Double]](
      (cv: (Int,Double)) => { val m = scala.collection.mutable.Map[Int,Double](); m.update(cv._1, cv._2); m },
      (m:  scala.collection.mutable.Map[Int,Double], cv: (Int,Double)) => { m.update(cv._1, cv._2); m },
      (m1: scala.collection.mutable.Map[Int,Double], m2: scala.collection.mutable.Map[Int,Double]) => {
        if (m2.size > m1.size) { m2 ++= m1; m2 } else { m1 ++= m2; m1 }
      }
    )

    val allRows = sc.parallelize(0 until aRows, P).map(i => (i, ()))
    val denseLines = allRows.leftOuterJoin(rowMap)
      .sortByKey(numPartitions = P)
      .map { case (_, (_, mOpt)) =>
        val m = mOpt.getOrElse(scala.collection.mutable.Map.empty[Int,Double])
        val sb = new StringBuilder
        var j = 0
        while (j < bCols) {
          if (j > 0) sb.append(", ")
          sb.append(m.getOrElse(j, 0.0))
          j += 1
        }
        sb.toString
      }

    denseLines.coalesce(1, shuffle = true)
      .mapPartitions(it => Iterator.single(it.mkString("\n")))
      .first()
  }

  // ============ UI 组件 ============
  val leftArea  = new TextArea { text = "4,0,9,0,0\n0,7,0,0,0\n0,0,0,0,0\n0,0,0,5,0"; rows = 12; columns = 36 }
  val rightArea = new TextArea { text = "1,0,0,0\n0,0,4,1\n2,0,0,0\n3,0,0,1\n0,0,2,0"; rows = 12; columns = 36 }

  val rddResultArea = new TextArea { editable=false; rows=10; columns=90; text="RDD 结果预览" }
  val dfResultArea  = new TextArea { editable=false; rows=10; columns=90; text="DataFrame 结果预览" }
  val rddMetricsArea= new TextArea { editable=false; rows=10; columns=90; text="RDD 指标" }
  val dfMetricsArea = new TextArea { editable=false; rows=10; columns=90; text="DataFrame 指标" }

  val leftShape    = new Label("左形状：-")
  val rightShape   = new Label("右形状：-")
  val dimCheck     = new Label("维度检查：-")
  val statusLabel  = new Label("状态：就绪")
  val timeLabel    = new Label("耗时：-")

  val runBtn       = new Button("计算（仅 RDD）")
  val runBothBtn   = new Button("计算并对比（RDD + DataFrame）")
  val clearBtn     = new Button("清空")
  val loadLeft     = new Button("载入左矩阵…")
  val loadRight    = new Button("载入右矩阵/向量…")
  val saveInput    = new Button("保存输入…")
  val saveRes      = new Button("保存结果…")
  val saveMetrics  = new Button("保存指标…")

  val previewRowsField = new TextField("20", 5)
  val previewColsField = new TextField("120", 5)
  val progress = new ProgressBar { min=0; max=100; value=0; indeterminate=false; preferredSize=new Dimension(180,16) }

  // ============ 辅助格式 ============
  private def humanBytes(b: Long): String = {
    val kb = 1024.0; val mb = kb*1024; val gb = mb*1024
    if (b >= gb) f"${b/gb}%.2f GB" else if (b >= mb) f"${b/mb}%.2f MB"
    else if (b >= kb) f"${b/kb}%.2f KB" else s"${b} B"
  }
  private def q(p: Double, xs: Vector[Long]): Long = {
    if (xs.isEmpty) 0L
    else {
      val s   = xs.sorted
      val n   = s.length
      val pos = (n - 1) * p
      val idx = math.max(0, math.min(n - 1, math.round(pos).toInt)) // 夹到 [0, n-1]
      s(idx)
    }
  }
  private def formatMetrics(label: String, aShape:(Int,Int), bShape:(Int,Int), s: MetricsCollector.Snapshot): String = {
    val p50=q(0.50,s.taskTimesMs); val p90=q(0.90,s.taskTimesMs); val p99=q(0.99,s.taskTimesMs)
    val max= if (s.taskTimesMs.isEmpty) 0L else s.taskTimesMs.max
    val rB = if (bShape._1==0 && bShape._2==0) s"x=|${aShape._2}|" else s"B=${bShape._1}x${bShape._2}"
    s"""|[$label]  A=${aShape._1}x${aShape._2} , $rB
        |Wall time   : ${"%.1f".format(s.wallMs)} ms
        |Jobs/Stages/Tasks : ${s.jobs}/${s.stages}/${s.tasks}
        |Shuffle      : read ${humanBytes(s.shuffleRead)} , write ${humanBytes(s.shuffleWrite)}
        |IO           : input ${humanBytes(s.inputBytes)} , output ${humanBytes(s.outputBytes)}
        |Spill        : memory ${humanBytes(s.memSpill)} , disk ${humanBytes(s.diskSpill)}
        |JVM GC       : ${s.gcTimeMs} ms
        |Task runtime : p50=${p50} ms , p90=${p90} ms , p99=${p99} ms , max=${max} ms
        |""".stripMargin
  }

  private def refreshMeta(): Unit = {
    val L = inferShape(normalizeToCsv(leftArea.text))
    val R = inferShape(normalizeToCsv(rightArea.text))
    def ok(l:Label)= l.foreground=new Color(0,128,0)
    def bad(l:Label)= l.foreground=Color.RED
    L match { case Right((r,c)) => leftShape.text=s"左形状：$r x $c"; ok(leftShape); case Left(m)=> leftShape.text=s"左形状：$m"; bad(leftShape) }
    R match { case Right((r,c)) => rightShape.text=s"右形状：$r x $c"; ok(rightShape); case Left(m)=> rightShape.text=s"右形状：$m"; bad(rightShape) }
    (L,R) match {
      case (Right((_,ac)),Right((br,_))) =>
        val pass=ac==br; dimCheck.text= if(pass) s"维度检查：OK（A.cols=$ac == B.rows=$br）" else s"维度检查：不匹配（A.cols=$ac ≠ B.rows=$br）"
        if(pass) ok(dimCheck) else bad(dimCheck)
      case _ => dimCheck.text="维度检查：-"; dimCheck.foreground=Color.DARK_GRAY
    }
  }

  private def setBusy(b:Boolean): Unit = {
    runBtn.enabled = !b; runBothBtn.enabled = !b; clearBtn.enabled = !b
    loadLeft.enabled = !b; loadRight.enabled = !b; saveInput.enabled = !b
    saveRes.enabled = !b; saveMetrics.enabled = !b; progress.indeterminate = b
    statusLabel.text = if (b) "状态：计算中…" else "状态：就绪"
  }

  private def chooseFile(toSave:Boolean,title:String):Option[File]={
    val fc=new FileChooser(new File(".")); fc.title=title; fc.fileSelectionMode=FileChooser.SelectionMode.FilesOnly
    val res= if(toSave) fc.showSaveDialog(null) else fc.showOpenDialog(null)
    if(res==FileChooser.Result.Approve) Option(fc.selectedFile) else None
  }

  private def saveInputToFiles(): Unit = {
    chooseFile(toSave=true,"保存当前输入为 CSV（目录将包含 left/right）…").foreach { f =>
      val base = if (f.isDirectory) f else f.getParentFile
      writeCSV(normalizeToCsv(leftArea.text),  new File(base,"left_matrix.csv"))
      writeCSV(normalizeToCsv(rightArea.text), new File(base,"right_matrix.csv"))
      Dialog.showMessage(null, s"输入已保存到：${base.getAbsolutePath}", "成功", Dialog.Message.Info)
    }
  }

  private def saveResultToFile(): Unit = {
    // 直接做类型匹配，避免使用 collect
    val rddAllOpt: Option[String] = rddResultArea.peer.getClientProperty("FULL_RESULT_STRING") match {
      case s: String => Some(s)
      case _         => None
    }
    val dfAllOpt: Option[String] = dfResultArea.peer.getClientProperty("FULL_RESULT_STRING") match {
      case s: String => Some(s)
      case _         => None
    }

    if (rddAllOpt.isEmpty && dfAllOpt.isEmpty) {
      Dialog.showMessage(null, "没有可保存的结果（请先计算）。", "提示", Dialog.Message.Info)
      return
    }

    chooseFile(toSave = true, "保存结果到…").foreach { f =>
      val base = if (f.isDirectory) f else f.getParentFile

      rddAllOpt.foreach { s =>
        val out = new File(base, "result_rdd.csv")
        val bw  = new BufferedWriter(new FileWriter(out))
        try bw.write(s) finally bw.close()
      }
      dfAllOpt.foreach { s =>
        val out = new File(base, "result_df.csv")
        val bw  = new BufferedWriter(new FileWriter(out))
        try bw.write(s) finally bw.close()
      }

      Dialog.showMessage(null,
        s"结果已保存到：${base.getAbsolutePath}\n(result_rdd.csv / result_df.csv)",
        "成功",
        Dialog.Message.Info
      )
    }
  }


  private def saveMetricsToFile(): Unit = {
    val rddM = rddMetricsArea.text; val dfM = dfMetricsArea.text
    if ((rddM.trim.isEmpty || rddM=="RDD 指标") && (dfM.trim.isEmpty || dfM=="DataFrame 指标")) {
      Dialog.showMessage(null, "没有可保存的指标（请先计算）。", "提示", Dialog.Message.Info); return
    }
    chooseFile(toSave=true,"保存指标到…").foreach { f =>
      val base = if (f.isDirectory) f else f.getParentFile
      val wr1 = new BufferedWriter(new FileWriter(new File(base,"metrics_rdd.txt"))); try wr1.write(rddM) finally wr1.close()
      val wr2 = new BufferedWriter(new FileWriter(new File(base,"metrics_df.txt")));  try wr2.write(dfM)  finally wr2.close()
      Dialog.showMessage(null, s"指标已保存到：${base.getAbsolutePath}\n(metrics_rdd.txt / metrics_df.txt)", "成功", Dialog.Message.Info)
    }
  }

  private def runRDDOnly(): Unit = runCore(compareDF=false)
  private def runBoth(): Unit   = runCore(compareDF=true)

  private def runCore(compareDF:Boolean): Unit = {
    val leftCsv  = normalizeToCsv(leftArea.text)
    val rightCsv = normalizeToCsv(rightArea.text)

    val L = inferShape(leftCsv); val R = inferShape(rightCsv)
    val (aRows, aCols) = L.getOrElse { Dialog.showMessage(null,s"左侧输入错误：${L.left.get}","错误",Dialog.Message.Error); return }
    val (bRows, bCols) = R.getOrElse { Dialog.showMessage(null,s"右侧输入错误：${R.left.get}","错误",Dialog.Message.Error); return }
    if (aCols != bRows) { Dialog.showMessage(null,s"维度不匹配：A.cols=$aCols, B.rows=$bRows","错误",Dialog.Message.Error); return }

    MetricsCollector.install(Run.sc) // 确保 listener 已注册

    setBusy(true); timeLabel.text="耗时：-"; progress.value=0
    rddResultArea.text="RDD 结果预览"; dfResultArea.text="DataFrame 结果预览"
    rddMetricsArea.text="RDD 指标"; dfMetricsArea.text="DataFrame 指标"

    Future {
      // ---- 写临时文件 ----
      val leftFile  = File.createTempFile("left_matrix_", ".csv")
      val rightFile = File.createTempFile("right_matrix_", ".csv")
      leftFile.deleteOnExit(); rightFile.deleteOnExit()
      writeCSV(leftCsv, leftFile); writeCSV(rightCsv, rightFile)

      // ---- 跑 RDD（你的 Runner）----
      MetricsCollector.reset()
      val t0 = System.nanoTime()
      val rddResult = Run.Run(leftFile.getAbsolutePath, rightFile.getAbsolutePath)
      val rddWallMs = (System.nanoTime()-t0)/1e6
      val rddSnap = MetricsCollector.snapshot("RDD", rddWallMs)

      // 预览裁剪
      val nRows  = Try(previewRowsField.text.trim.toInt).getOrElse(20).max(1)
      val nColsC = Try(previewColsField.text.trim.toInt).getOrElse(120).max(20)
      val rddPreview = rddResult.split("\\r?\\n").take(nRows).map { line =>
        if (line.length <= nColsC) line else line.take(nColsC) + " …"
      }.mkString("\n")

      // ---- 跑 DF（可选）----
      val dfStuff: Option[(String,String,MetricsCollector.Snapshot)] =
        if (!compareDF) None
        else {
          // 为公平与独立统计，再次 reset 指标
          MetricsCollector.reset()
          val t1 = System.nanoTime()
          val dfResult =
            if (bRows == 1)
              dfSpmvToString(leftFile.getAbsolutePath, rightFile.getAbsolutePath)  // 向量仍然返回单行 CSV
            else
              dfSpmmToDenseString(leftFile.getAbsolutePath, rightFile.getAbsolutePath, aRows = aRows, bCols = bCols)

          val dfWallMs = (System.nanoTime()-t1)/1e6
          val dfSnap = MetricsCollector.snapshot("DataFrame", dfWallMs)
          val dfPreview = dfResult.split("\\r?\\n").take(nRows).map { line =>
            if (line.length <= nColsC) line else line.take(nColsC) + " …"
          }.mkString("\n")
          Some((dfResult, dfPreview, dfSnap))
        }

      // 清理临时文件
      leftFile.delete(); rightFile.delete()

      // 回 UI
      (rddResult, rddPreview, rddSnap, dfStuff)
    }.map { case (rddResult, rddPreview, rddSnap, dfStuff) =>
      // 显示 RDD
      rddResultArea.text = rddPreview
      rddResultArea.peer.putClientProperty("FULL_RESULT_STRING", rddResult)
      rddMetricsArea.text = formatMetrics("RDD", (inferShape(normalizeToCsv(leftArea.text)).right.get._1, inferShape(normalizeToCsv(leftArea.text)).right.get._2),
        if (inferShape(normalizeToCsv(rightArea.text)).right.get._1==1) (0,0) else inferShape(normalizeToCsv(rightArea.text)).right.get, rddSnap)

      // 显示 DF
      dfStuff match {
        case Some((dfResult, dfPreview, dfSnap)) =>
          dfResultArea.text = dfPreview
          dfResultArea.peer.putClientProperty("FULL_RESULT_STRING", dfResult)
          dfMetricsArea.text = formatMetrics("DataFrame",
            (inferShape(normalizeToCsv(leftArea.text)).right.get._1, inferShape(normalizeToCsv(leftArea.text)).right.get._2),
            if (inferShape(normalizeToCsv(rightArea.text)).right.get._1==1) (0,0) else inferShape(normalizeToCsv(rightArea.text)).right.get,
            dfSnap)
          timeLabel.text = f"耗时：RDD ${rddSnap.wallMs}%.1f ms   /   DF ${dfSnap.wallMs}%.1f ms"
        case None =>
          dfResultArea.text = "DataFrame 结果预览（未运行）"
          dfMetricsArea.text = "DataFrame 指标（未运行）"
          timeLabel.text = f"耗时：RDD ${rddSnap.wallMs}%.1f ms"
      }
      statusLabel.text = "状态：完成"
      setBusy(false)
    }.recover { case e =>
      rddResultArea.text = s"计算失败：${e.getMessage}"
      statusLabel.text = "状态：失败"
      setBusy(false)
    }
  }

  // ============ 首次预热（warm-up）：缓解第一次计算过慢 ============
  private def warmUpOnce(): Unit = {
    Future {
      try {
        // 轻量 RDD 任务（仅触发 executor/线程池/JIT 等初始化）
        Run.sc.parallelize(1 to 10).count()

        // 轻量 DF 任务（复用现有 SparkSession），不用 collect
        val sp = spark()
        import sp.implicits._
        import org.apache.spark.sql.functions.sum
        sp.range(0, 1000).select(sum('id)).count()   // 仅计数1行，触发 Catalyst/Codegen/AQE
      } catch { case _: Throwable => () }
    }
  }


  // ============ 顶层 UI ============
  def top: MainFrame = new MainFrame {
    title = "PDSS CW1 — Matrix Frontend (Pure RDD vs DataFrame)"
    preferredSize = new Dimension(1200, 900)

    // 菜单栏
    menuBar = new MenuBar {
      contents += new Menu("文件") {
        contents += new MenuItem(Action("保存结果…")(saveResultToFile()))
        contents += new MenuItem(Action("保存指标…")(saveMetricsToFile()))
        contents += new MenuItem(Action("保存输入…")(saveInputToFiles()))
        contents += new Separator
        contents += new MenuItem(Action("退出"){ quit() })
      }
    }

    contents = new BorderPanel {
      layout(new BoxPanel(Orientation.Horizontal) {
        contents += new BoxPanel(Orientation.Vertical) {
          contents += new Label("Left Matrix (A)")
          contents += new ScrollPane(leftArea) { preferredSize = new Dimension(560, 260) }
        }
        contents += Swing.HStrut(10)
        contents += new BoxPanel(Orientation.Vertical) {
          contents += new Label("Right Matrix / Vector (B / x)")
          contents += new ScrollPane(rightArea) { preferredSize = new Dimension(560, 260) }
        }
      }) = BorderPanel.Position.Center

      layout(new BoxPanel(Orientation.Vertical) {
        contents += new FlowPanel(
          new Label("预览前 N 行："), previewRowsField,
          new Label("每行最多 N 字符："), previewColsField,
          Swing.HStrut(16),
          runBtn, runBothBtn, clearBtn, loadLeft, loadRight, saveInput, saveRes, saveMetrics,
          Swing.HStrut(16), progress
        )
        contents += new FlowPanel(leftShape, Swing.HStrut(20), rightShape, Swing.HStrut(20),
          dimCheck, Swing.HStrut(20), timeLabel, Swing.HStrut(20), statusLabel)

        // 上：结果对比；下：指标对比
        contents += new GridPanel(2,1) {
          contents += new GridPanel(1,2) {
            border = Swing.TitledBorder(Swing.LineBorder(Color.GRAY), "结果预览")
            contents += new ScrollPane(rddResultArea) { preferredSize = new Dimension(560, 220) }
            contents += new ScrollPane(dfResultArea)  { preferredSize = new Dimension(560, 220) }
          }
          contents += new GridPanel(1,2) {
            border = Swing.TitledBorder(Swing.LineBorder(Color.GRAY), "Metrics（指标）")
            contents += new ScrollPane(rddMetricsArea) { preferredSize = new Dimension(560, 220) }
            contents += new ScrollPane(dfMetricsArea)  { preferredSize = new Dimension(560, 220) }
          }
        }
      }) = BorderPanel.Position.South
    }

    // 事件绑定
    listenTo(runBtn, runBothBtn, clearBtn, loadLeft, loadRight, saveInput, saveRes, saveMetrics)
    reactions += {
      case ButtonClicked(`runBtn`)     => refreshMeta(); runRDDOnly()
      case ButtonClicked(`runBothBtn`) => refreshMeta(); runBoth()
      case ButtonClicked(`clearBtn`)   =>
        leftArea.text = ""; rightArea.text = ""
        rddResultArea.text = "RDD 结果预览"; dfResultArea.text = "DataFrame 结果预览"
        rddMetricsArea.text = "RDD 指标";   dfMetricsArea.text = "DataFrame 指标"
        leftShape.text="左形状：-"; rightShape.text="右形状：-"
        dimCheck.text="维度检查：-"; dimCheck.foreground=Color.DARK_GRAY
        timeLabel.text="耗时：-"; statusLabel.text="状态：就绪"
      case ButtonClicked(`loadLeft`)   =>
        chooseFile(toSave=false,"选择左矩阵 CSV…").foreach { f => leftArea.text = readFileToText(f); refreshMeta() }
      case ButtonClicked(`loadRight`)  =>
        chooseFile(toSave=false,"选择右矩阵/向量 CSV…").foreach { f => rightArea.text = readFileToText(f); refreshMeta() }
      case ButtonClicked(`saveInput`)  => saveInputToFiles()
      case ButtonClicked(`saveRes`)    => saveResultToFile()
      case ButtonClicked(`saveMetrics`)=> saveMetricsToFile()
    }

    // 快捷键：Ctrl+R 只跑 RDD；Ctrl+D 对比跑；Ctrl+S 保存结果；Ctrl+M 保存指标
    peer.getRootPane.registerKeyboardAction((_:ActionEvent)=>runRDDOnly(),
      javax.swing.KeyStroke.getKeyStroke(KeyEvent.VK_R, InputEvent.CTRL_DOWN_MASK),
      javax.swing.JComponent.WHEN_IN_FOCUSED_WINDOW)
    peer.getRootPane.registerKeyboardAction((_:ActionEvent)=>runBoth(),
      javax.swing.KeyStroke.getKeyStroke(KeyEvent.VK_D, InputEvent.CTRL_DOWN_MASK),
      javax.swing.JComponent.WHEN_IN_FOCUSED_WINDOW)
    peer.getRootPane.registerKeyboardAction((_:ActionEvent)=>saveResultToFile(),
      javax.swing.KeyStroke.getKeyStroke(KeyEvent.VK_S, InputEvent.CTRL_DOWN_MASK),
      javax.swing.JComponent.WHEN_IN_FOCUSED_WINDOW)
    peer.getRootPane.registerKeyboardAction((_:ActionEvent)=>saveMetricsToFile(),
      javax.swing.KeyStroke.getKeyStroke(KeyEvent.VK_M, InputEvent.CTRL_DOWN_MASK),
      javax.swing.JComponent.WHEN_IN_FOCUSED_WINDOW)

    // 初始刷新 & 预热：缓解第一次极慢
    refreshMeta()
    warmUpOnce()

    // 关闭窗口 → 停 Spark
    override def closeOperation(): Unit = {
      try { println("Shutting down SparkContext..."); Run.sc.stop() }
      finally { super.closeOperation() }
    }

    centerOnScreen()
  }
}

