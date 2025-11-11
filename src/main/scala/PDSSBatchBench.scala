import java.io._
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}
import scala.util.Try
import scala.concurrent.duration._

import org.apache.spark.SparkContext
import org.apache.spark.scheduler._
import org.apache.spark.sql.{SparkSession, Dataset, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.HashPartitioner

/** 批量测试入口：一次执行，自动跑完 RDD vs DataFrame，对比并落盘 */
object PDSSBatchBench {

  // --------------------- Runner（RDD 实现） ---------------------
  // 复用你项目里的 Runner；它会创建并持有一个 SparkContext
  val Run = new Runner

  // --------------------- DF 对照（与前端一致） ------------------
  case class Entry(i: Int, j: Int, v: Double)
  case class Vec(k: Int, x: Double)

  private def spark(): SparkSession =
    SparkSession.builder().config(Run.sc.getConf).getOrCreate()

  /** 把稠密 CSV 读成 COO(i,j,v)（忽略 0） */
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

  /** 读行向量（1×N）；若不是单行会失败（列向量场景我们走 SpMM） */
  private def dfReadVectorRow(path: String): Dataset[Vec] = {
    val sp = spark(); import sp.implicits._
    val first = Run.sc.textFile(path).filter(_.trim.nonEmpty).first()
    val toks  = first.split(",").map(_.trim).filter(_.nonEmpty)
    val vec   = toks.zipWithIndex.flatMap { case (s, idx) => Try(s.toDouble).toOption.map(d => Vec(idx, d)) }
    sp.createDataset(vec.toSeq)
  }

  /** DataFrame：A·x → 单行 CSV 字符串（与前端一致） */
  private def dfSpmvToString(pathA: String, pathX: String): String = {
    val sp = spark(); import sp.implicits._
    val A = dfReadDenseCsvAsCOO(pathA)         // Entry(i,j,v)
    val x = dfReadVectorRow(pathX)             // Vec(k,x) —— 行向量
    val Aj = A.withColumnRenamed("j","k").withColumnRenamed("v","aik")
    val X  = x.toDF("k","x")
    val dfY = Aj.join(X, Seq("k"))
      .select(col("i"), (col("aik")*col("x")).as("prod"))
      .groupBy("i").agg(sum("prod").as("y"))
      .orderBy("i")
      .select(col("y"))
    val onePart = dfY.rdd.map(_.getDouble(0).toString)
      .coalesce(1, shuffle = true).mapPartitions(it => Iterator.single(it.mkString(",")))
    onePart.first()
  }

  /** DataFrame：A·B → 稠密化为逐行 CSV（与前端一致的“先 COO 再稠密”） */
  private def dfSpmmToDenseString(pathA: String, pathB: String, aRows: Int, bCols: Int,
                                  previewLines: Option[Int] = None): String = {
    val sp = spark(); import sp.implicits._
    val A = dfReadDenseCsvAsCOO(pathA)
    val B = dfReadDenseCsvAsCOO(pathB)
    val Ak = A.withColumnRenamed("j","k").withColumnRenamed("v","aik")
    val Bk = B.withColumnRenamed("i","k").withColumnRenamed("v","kbj")
    val dfC = Ak.join(Bk, Seq("k"))
      .select(col("i"), col("j"), (col("aik")*col("kbj")).as("prod"))
      .groupBy("i","j").agg(sum("prod").as("cij"))

    // 稠密化（复用 RDD 逻辑，与前端一致）
    val sc = Run.sc
    val P  = sc.defaultParallelism
    val part = new HashPartitioner(P)

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

    // 大结果可选择预览 N 行；否则生成完整字符串
    previewLines match {
      case Some(n) =>
        denseLines.take(n).mkString("\n")
      case None =>
        denseLines.coalesce(1, shuffle = true)
          .mapPartitions(it => Iterator.single(it.mkString("\n")))
          .first()
    }
  }

  // --------------------- 指标采集（与前端同口径） ---------------------
  object Metrics extends SparkListener {
    case class Snapshot(label: String,
                        wallMs: Double,
                        jobs: Int, stages: Int, tasks: Int,
                        shuffleRead: Long, shuffleWrite: Long,
                        inputBytes: Long, outputBytes: Long,
                        memSpill: Long, diskSpill: Long,
                        gcTimeMs: Long,
                        taskTimesMs: Vector[Long]) {
      def toPretty(aShape: (Int,Int), bShape: (Int,Int)): String = {
        def hb(b: Long): String = {
          val kb=1024.0; val mb=kb*1024; val gb=mb*1024
          if (b >= gb) f"${b/gb}%.2f GB" else if (b >= mb) f"${b/mb}%.2f MB"
          else if (b >= kb) f"${b/kb}%.2f KB" else s"${b} B"
        }
        val p50=q(0.50, taskTimesMs); val p90=q(0.90, taskTimesMs); val p99=q(0.99, taskTimesMs); val mx = if (taskTimesMs.isEmpty) 0L else taskTimesMs.max
        val rB = if (bShape._1==0 && bShape._2==0) s"x=|${aShape._2}|" else s"B=${bShape._1}x${bShape._2}"
        s"""|[$label]  A=${aShape._1}x${aShape._2} , $rB
            |Wall time   : ${"%.1f".format(wallMs)} ms
            |Jobs/Stages/Tasks : $jobs/$stages/$tasks
            |Shuffle      : read ${hb(shuffleRead)} , write ${hb(shuffleWrite)}
            |IO           : input ${hb(inputBytes)} , output ${hb(outputBytes)}
            |Spill        : memory ${hb(memSpill)} , disk ${hb(diskSpill)}
            |JVM GC       : $gcTimeMs ms
            |Task runtime : p50=$p50 ms , p90=$p90 ms , p99=$p99 ms , max=$mx ms
            |""".stripMargin
      }
    }
    private var installed = false
    private var jobs = 0; private var stages = 0; private var tasks = 0
    private var shuffleRead = 0L; private var shuffleWrite = 0L
    private var inputBytes = 0L; private var outputBytes = 0L
    private var memSpill = 0L; private var diskSpill = 0L
    private var gcTime = 0L
    private var taskTimes = Vector.empty[Long]
    def install(sc: SparkContext): Unit = synchronized { if (!installed) { sc.addSparkListener(this); installed = true } }
    def reset(): Unit = synchronized {
      jobs=0; stages=0; tasks=0; shuffleRead=0L; shuffleWrite=0L
      inputBytes=0L; outputBytes=0L; memSpill=0L; diskSpill=0L; gcTime=0L; taskTimes=Vector.empty
    }
    def snapshot(label: String, wallMs: Double): Snapshot = synchronized {
      Snapshot(label, wallMs, jobs, stages, tasks, shuffleRead, shuffleWrite, inputBytes, outputBytes, memSpill, diskSpill, gcTime, taskTimes)
    }
    override def onJobStart(e: SparkListenerJobStart): Unit = synchronized { jobs += 1 }
    override def onStageCompleted(e: SparkListenerStageCompleted): Unit = synchronized { stages += 1 }
    override def onTaskEnd(e: SparkListenerTaskEnd): Unit = synchronized {
      val m = e.taskMetrics; tasks += 1
      if (m != null) {
        taskTimes :+= m.executorRunTime
        gcTime += m.jvmGCTime
        if (m.inputMetrics != null) inputBytes += m.inputMetrics.bytesRead
        if (m.outputMetrics != null) outputBytes += m.outputMetrics.bytesWritten
        if (m.shuffleReadMetrics != null) shuffleRead += m.shuffleReadMetrics.totalBytesRead
        if (m.shuffleWriteMetrics != null) shuffleWrite += m.shuffleWriteMetrics.bytesWritten
        memSpill += m.memoryBytesSpilled; diskSpill += m.diskBytesSpilled
      }
    }
    private def q(p: Double, xs: Vector[Long]): Long = {
      if (xs.isEmpty) 0L
      else {
        val s = xs.sorted
        val n = s.length
        val pos = (n - 1) * p
        val idx = math.max(0, math.min(n - 1, math.round(pos).toInt))
        s(idx)
      }
    }
  }

  // --------------------- 文件与测试用例 ---------------------

  case class TestCase(name: String, left: String, right: String,
                      expectRows: Option[Int] = None, expectCols: Option[Int] = None)

  // small
  val smallCases = Seq(
    TestCase("small_spmv_3x3", "test_data/small/matrix_3x3_sparse.csv", "test_data/small/vector_3.csv", Some(3), Some(1)),
    TestCase("small_spmm_3x3", "test_data/small/matrix_3x3_sparse.csv", "test_data/small/matrix_3x3_dense.csv", Some(3), Some(3)),
    TestCase("small_identity_rect", "test_data/small/identity_5x5.csv", "test_data/small/rect_5x3.csv", Some(5), Some(3)),
    TestCase("small_zero_x_diag", "test_data/small/zero_4x4.csv", "test_data/small/diagonal_4x4.csv", Some(4), Some(4))
  )

  // medium
  val mediumCases = Seq(
    TestCase("med_100x100_1pct_x_100",  "test_data/medium/matrix_100x100_1pct.csv",  "test_data/medium/vector_100.csv",  Some(100), Some(1)),
    TestCase("med_100x100_5pct_x_100",  "test_data/medium/matrix_100x100_5pct.csv",  "test_data/medium/vector_100.csv",  Some(100), Some(1)),
    TestCase("med_100x100_10pct_x_100", "test_data/medium/matrix_100x100_10pct.csv", "test_data/medium/vector_100.csv",  Some(100), Some(1)),
    TestCase("med_100x100_30pct_x_100", "test_data/medium/matrix_100x100_30pct.csv", "test_data/medium/vector_100.csv",  Some(100), Some(1)),
    TestCase("med_rect_50x100_x_100",   "test_data/medium/matrix_50x100_5pct.csv",   "test_data/medium/vector_100.csv",  Some(50),  Some(1)),
    TestCase("med_rect_100x50_mul",     "test_data/medium/matrix_100x50_5pct.csv",   "test_data/medium/matrix_50x100_5pct.csv", Some(100), Some(100))
  )

  // large（向量为列向量 → 前端/DF 会按矩阵-矩阵跑出  n×1 ）
  val largeCases = Seq(
    TestCase("lg_1k_1pct_x_1k",    "test_data/large/matrix_1k_1pct.csv",    "test_data/large/vector_1000.csv",  Some(1000), Some(1)),
    TestCase("lg_5k_0.1pct_x_5k",  "test_data/large/matrix_5k_0.1pct.csv",  "test_data/large/vector_5000.csv",  Some(5000), Some(1)),
    TestCase("lg_10k_0.1pct_x_10k","test_data/large/matrix_10k_0.1pct.csv", "test_data/large/vector_10000.csv", Some(10000), Some(1))
  )

  // patterns
  val patternCases = Seq(
    TestCase("pat_tridiag_1000_x",     "test_data/patterns/tridiagonal_1000.csv",      "test_data/large/vector_1000.csv",  Some(1000), Some(1)),
    TestCase("pat_banded10_1000_x",    "test_data/patterns/banded_1000_width10.csv",   "test_data/large/vector_1000.csv",  Some(1000), Some(1)),
    TestCase("pat_blockdiag_1000_x",   "test_data/patterns/block_diagonal_1000.csv",   "test_data/large/vector_1000.csv",  Some(1000), Some(1)),
    TestCase("pat_powerlaw_1000_x",    "test_data/patterns/power_law_1000.csv",        "test_data/large/vector_1000.csv",  Some(1000), Some(1)),
    TestCase("pat_rowhot_1000_x",      "test_data/patterns/row_hotspot_1000.csv",      "test_data/large/vector_1000.csv",  Some(1000), Some(1))
  )

  // benchmark（示范两条；其余可按需追加）
  val benchCases = Seq(
    TestCase("bench_scale_1000",   "test_data/benchmark/scale_1000x1000.csv", "test_data/benchmark/vector_1000.csv", Some(1000), Some(1)),
    TestCase("bench_sparsity_2000_1pct", "test_data/benchmark/sparsity_2000x2000_1_0pct.csv", "test_data/benchmark/vector_2000.csv", Some(2000), Some(1))
  )

  // --------------------- 辅助：形状与输出策略 ---------------------
  private def nowTag(): String = {
    val z = Instant.now().atZone(ZoneId.systemDefault())
    z.format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"))
  }
  private def ensureDir(d: File): Unit = { if (!d.exists()) d.mkdirs() }

  private def preview(s: String, nLines: Int): String =
    s.split("\\r?\\n").take(nLines).mkString("\n")

  // 大结果策略：超过此元素数就不保存“完整结果字符串”，只保存预览
  val MaxCellsForFullSave: Long = 2_000_000L
  val PreviewLinesForHuge     : Int  = 100

  // --------------------- 执行单个用例 ---------------------
  private def runCase(tc: TestCase, outRoot: File): Unit = {
    val outDir = new File(outRoot, tc.name); ensureDir(outDir)
    println(s"\n== Running: ${tc.name} ==")

    // 估形状（只读少量行，不加载全文件）
    def inferShape(path: String): (Int, Int) = {
      val src = scala.io.Source.fromFile(path)
      try {
        val it = src.getLines()
        var rows = 0; var minC = Int.MaxValue; var maxC = Int.MinValue
        while (it.hasNext) {
          val line = it.next().trim
          if (line.nonEmpty) {
            val c = line.split(",").count(_.trim.nonEmpty)
            rows += 1
            if (c < minC) minC = c
            if (c > maxC) maxC = c
          }
        }
        val cols = if (rows == 0) 0 else minC
        (rows, cols)
      } finally src.close()
    }
    val aShape = inferShape(tc.left)
    val bShape = inferShape(tc.right)
    println(s"A shape = ${aShape._1} x ${aShape._2} , B shape = ${bShape._1} x ${bShape._2}")

    // 指标监听
    Metrics.install(Run.sc)

    // ---------- RDD ----------
    Metrics.reset()
    val t0 = System.nanoTime()
    val rddResult = Run.Run(tc.left, tc.right)
    val rddWallMs = (System.nanoTime() - t0) / 1e6
    val rddSnap   = Metrics.snapshot("RDD", rddWallMs)

    // 保存 RDD 结果（按策略）
    val rddCells = aShape._1.toLong * (if (bShape._2 == 0) 1L else bShape._2.toLong)
    val rddResultFile =
      if (rddCells <= MaxCellsForFullSave) new File(outDir, "result_rdd.csv")
      else new File(outDir, "result_rdd_preview.csv")
    writeString(rddResultFile, if (rddCells <= MaxCellsForFullSave) rddResult else preview(rddResult, PreviewLinesForHuge))

    writeString(new File(outDir, "metrics_rdd.txt"), rddSnap.toPretty(aShape, if (bShape._1==1) (0,0) else bShape))

    // ---------- DataFrame ----------
    Metrics.reset()
    val t1 = System.nanoTime()
    val dfIsVectorRow = (bShape._1 == 1) // 单行 → 行向量；多行单列 → 列向量，需要走 SpMM
    val dfResult: String =
      if (dfIsVectorRow)
        dfSpmvToString(tc.left, tc.right)
      else {
        val dfCells = aShape._1.toLong * bShape._2.toLong
        val previewOpt = if (dfCells > MaxCellsForFullSave) Some(PreviewLinesForHuge) else None
        dfSpmmToDenseString(tc.left, tc.right, aRows = aShape._1, bCols = bShape._2, previewLines = previewOpt)
      }
    val dfWallMs = (System.nanoTime() - t1) / 1e6
    val dfSnap   = Metrics.snapshot("DataFrame", dfWallMs)

    // 保存 DF 结果
    val dfCells = aShape._1.toLong * (if (bShape._2 == 0) 1L else bShape._2.toLong)
    val dfResultFile =
      if (dfCells <= MaxCellsForFullSave) new File(outDir, "result_df.csv")
      else new File(outDir, "result_df_preview.csv")
    writeString(dfResultFile, dfResult)

    writeString(new File(outDir, "metrics_df.txt"), dfSnap.toPretty(aShape, if (bShape._1==1) (0,0) else bShape))

    // ---------- 汇总 ----------
    appendSummary(outRoot, tc.name, aShape, bShape, rddSnap.wallMs, dfSnap.wallMs)
    println(f"Done: RDD ${rddSnap.wallMs}%.1f ms | DF ${dfSnap.wallMs}%.1f ms → ${outDir.getPath}")
  }

  // --------------------- 主流程：跑所有用例 ---------------------
  def main(args: Array[String]): Unit = {
    val outRoot = new File(s"runs/${nowTag()}"); ensureDir(outRoot)
    writeString(new File(outRoot, "info.txt"),
      s"""|PDSS Batch Bench
          |Timestamp: ${nowTag()}
          |""".stripMargin)

    // 写 summary 表头
    writeString(new File(outRoot, "summary.csv"), "name,A_rows,A_cols,B_rows,B_cols,RDD_ms,DF_ms\n", append = false)

    // 跑全部用例（可按需精简）
    (smallCases ++ mediumCases ++ largeCases ++ patternCases ++ benchCases).foreach { tc =>
      runCase(tc, outRoot)
    }

    println(s"\n✅ ALL DONE. See: ${outRoot.getAbsolutePath}")
  }

  // --------------------- I/O & 辅助 ---------------------
  private def writeString(file: File, content: String, append: Boolean = false): Unit = {
    val parent = file.getParentFile; if (parent != null && !parent.exists()) parent.mkdirs()
    val bw = new BufferedWriter(new FileWriter(file, append))
    try bw.write(content) finally bw.close()
  }

  private def appendSummary(root: File, name: String, aShape:(Int,Int), bShape:(Int,Int),
                            rddMs: Double, dfMs: Double): Unit = {
    val line = s"$name,${aShape._1},${aShape._2},${bShape._1},${bShape._2},${"%.1f".format(rddMs)},${"%.1f".format(dfMs)}\n"
    writeString(new File(root, "summary.csv"), line, append = true)
  }
}

