import java.io._
import java.util.zip.CRC32
import scala.util.Try
import scala.math._

import org.apache.spark.SparkContext
import org.apache.spark.scheduler._
import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.HashPartitioner

/** 批量测试（Total vs Kernel-only 两轮并行口径 + Trials + Ablation + E2E） */
object PDSSBatchBench {

  // --------------------- 配置 ---------------------
  val MaxCellsForFullSave: Long = 2_000_000L
  val PreviewLinesForHuge   : Int  = 100
  private val dataRootProp  = sys.props.getOrElse("pdss.data.dir", "test_data")

  // Runner（不改算子）
  val Run = new Runner

  // --------------------- DF 基线与工具 ---------------------
  case class Entry(i: Int, j: Int, v: Double)
  case class Vec(k: Int, x: Double)

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
  // 同时支持 1×N 行向量（逗号分隔一行）和 N×1 列向量（每行一个数）
  private def dfReadVectorRow(path: String): Dataset[Vec] = {
    val sp = spark(); import sp.implicits._
    val lines = Run.sc.textFile(path).filter(_.trim.nonEmpty)
    val head  = lines.first()
    if (head.contains(",")) {
      // 行向量
      val toks = head.split(",").map(_.trim).filter(_.nonEmpty)
      val vec  = toks.zipWithIndex.flatMap { case (s, idx) =>
        Try(s.toDouble).toOption.map(d => Vec(idx, d))
      }
      sp.createDataset(vec.toSeq)
    } else {
      // 列向量
      val rdd = lines.zipWithIndex().flatMap { case (s, idx) =>
        Try(s.trim.toDouble).toOption.map(d => Vec(idx.toInt, d))
      }
      sp.createDataset(rdd)
    }
  }

  // ---- 统一设置 DF 执行参数（与 kernel 配置保持一致）----
  private def tuneDF(sp: SparkSession, shuffleP: Int, noBroadcast: Boolean): Unit = {
    sp.conf.set("spark.sql.adaptive.enabled", "true")
    sp.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    sp.conf.set("spark.sql.shuffle.partitions", shuffleP.toString)
    sp.conf.set("spark.sql.autoBroadcastJoinThreshold",
      if (noBroadcast) "-1" else (10L << 20).toString) // 10MB
  }

  // --------------------- DF 端到端（用于 total） ---------------------
  private def dfSpmvToString(pathA: String, pathX: String,
                             ablation: Set[String]): String = {
    val sp = spark(); import sp.implicits._; import org.apache.spark.sql.functions._
    val P  = Run.sc.defaultParallelism.max(2)
    tuneDF(sp, if (ablation("df_shuffle_200")) 200 else P, ablation("df_no_broadcast"))

    val A = dfReadDenseCsvAsCOO(pathA)
    val X = dfReadVectorRow(pathX)
    val Aj = A.withColumnRenamed("j","k").withColumnRenamed("v","aik")
    val Xdf= if (ablation("df_no_broadcast")) X.toDF("k","x") else X.toDF("k","x").hint("broadcast")
    Aj.join(Xdf, Seq("k"))
      .select(col("i"), (col("aik")*col("x")).as("prod"))
      .groupBy("i").agg(sum("prod").as("y"))
      .orderBy("i").select(col("y"))
      .rdd.map(_.getDouble(0).toString)
      .coalesce(1, shuffle = true)
      .mapPartitions(it => Iterator.single(it.mkString(",")))
      .first()
  }

  private def dfSpmmToDenseString(pathA: String, pathB: String,
                                  aRows: Int, bCols: Int,
                                  previewLines: Option[Int],
                                  ablation: Set[String]): String = {
    val sp = spark(); import sp.implicits._; import org.apache.spark.sql.functions._
    val P  = Run.sc.defaultParallelism.max(2)
    tuneDF(sp, if (ablation("df_shuffle_200")) 200 else P, ablation("df_no_broadcast"))

    val A = dfReadDenseCsvAsCOO(pathA)
    val B = dfReadDenseCsvAsCOO(pathB)
    val Ak = A.withColumnRenamed("j","k").withColumnRenamed("v","aik")
    val Bk = B.withColumnRenamed("i","k").withColumnRenamed("v","kbj")
    val dfC = Ak.join(Bk, Seq("k"))
      .select(col("i"), col("j"), (col("aik")*col("kbj")).as("prod"))
      .groupBy("i","j").agg(sum("prod").as("cij"))

    val sc = Run.sc
    val part = new HashPartitioner(P)
    val rows = dfC.rdd.map(r => (r.getInt(0), (r.getInt(1), r.getDouble(2))))
    val rowMap = rows.partitionBy(part).combineByKey[scala.collection.mutable.Map[Int,Double]](
      (cv:(Int,Double)) => { val m=scala.collection.mutable.Map[Int,Double](); m.update(cv._1, cv._2); m },
      (m, cv)           => { m.update(cv._1, cv._2); m },
      (m1,m2)           => { if (m2.size>m1.size) { m2 ++= m1; m2 } else { m1 ++= m2; m1 } }
    )
    val allRows = sc.parallelize(0 until aRows, P).map(i => (i, ()))
    val denseLines = allRows.leftOuterJoin(rowMap)
      .sortByKey(numPartitions = P)
      .map { case (_, (_, mOpt)) =>
        val m = mOpt.getOrElse(scala.collection.mutable.Map.empty[Int,Double])
        val sb = new StringBuilder; var j=0
        while (j < bCols) { if (j>0) sb.append(", "); sb.append(m.getOrElse(j, 0.0)); j += 1 }
        sb.toString
      }

    previewLines match {
      case Some(n) =>
        denseLines
          .coalesce(1, shuffle = true)
          .mapPartitions { it =>
            var i = 0
            val sb = new StringBuilder
            while (it.hasNext && i < n) {
              if (i > 0) sb.append("\n")
              sb.append(it.next())
              i += 1
            }
            Iterator.single(sb.toString)
          }
          .first()
      case None =>
        denseLines
          .coalesce(1, shuffle = true)
          .mapPartitions(it => Iterator.single(it.mkString("\n")))
          .first()
    }
  }

  // --------------------- DF kernel-only ---------------------
  private def dfKernelMsSpmv(pathA: String, pathX: String,
                             ablation: Set[String]): Double = {
    val sp = spark(); import sp.implicits._; import org.apache.spark.sql.functions._
    val P  = Run.sc.defaultParallelism.max(2)
    tuneDF(sp, if (ablation("df_shuffle_200")) 200 else P, ablation("df_no_broadcast"))

    val A = dfReadDenseCsvAsCOO(pathA).cache(); A.count()
    val Xdf = dfReadVectorRow(pathX).toDF("k","x").hint("broadcast").cache()
    Xdf.count()

    val Aj = A.withColumnRenamed("j","k").withColumnRenamed("v","aik")
    val t0 = System.nanoTime()
    val dfY = Aj.join(Xdf, Seq("k"))
      .select(col("i"), (col("aik")*col("x")).as("prod"))
      .groupBy("i").agg(sum("prod").as("y"))
      .cache()
    dfY.count()
    val ms = (System.nanoTime() - t0) / 1e6

    dfY.unpersist(false); A.unpersist(false); Xdf.unpersist(false)
    ms
  }

  private def dfKernelMsSpmm(pathA: String, pathB: String,
                             ablation: Set[String]): Double = {
    val sp = spark(); import sp.implicits._; import org.apache.spark.sql.functions._
    val P  = Run.sc.defaultParallelism.max(2)
    tuneDF(sp, if (ablation("df_shuffle_200")) 200 else P, ablation("df_no_broadcast"))

    val A = dfReadDenseCsvAsCOO(pathA).cache(); A.count()
    val B = dfReadDenseCsvAsCOO(pathB).cache(); B.count()
    val Ak = A.withColumnRenamed("j","k").withColumnRenamed("v","aik")
    val Bk = B.withColumnRenamed("i","k").withColumnRenamed("v","kbj")

    val t0 = System.nanoTime()
    val dfC = Ak.join(Bk, Seq("k"))
      .select(col("i"), col("j"), (col("aik")*col("kbj")).as("prod"))
      .groupBy("i","j").agg(sum("prod").as("cij"))
      .cache()
    dfC.count()
    val ms = (System.nanoTime() - t0) / 1e6

    dfC.unpersist(false); A.unpersist(false); B.unpersist(false)
    ms
  }

  // --------------------- Metrics（用于 total pass） ---------------------
  object Metrics extends SparkListener {
    case class Snap(label:String, wall:Double,
                    jobs:Int, stages:Int, tasks:Int,
                    shR:Long, shW:Long, inB:Long, outB:Long,
                    mem:Long, disk:Long, gc:Long, times:Vector[Long])
    private var installed=false
    private var jobs=0; private var stages=0; private var tasks=0
    private var shR=0L; private var shW=0L; private var inB=0L; private var outB=0L
    private var mem=0L; private var disk=0L; private var gc=0L
    private var times=Vector.empty[Long]
    def install(sc: SparkContext): Unit = synchronized { if (!installed){ sc.addSparkListener(this); installed=true } }
    def reset(): Unit = synchronized { jobs=0; stages=0; tasks=0; shR=0L; shW=0L; inB=0L; outB=0L; mem=0L; disk=0L; gc=0L; times=Vector.empty }
    def snap(label:String, wall:Double) = synchronized { Snap(label, wall, jobs, stages, tasks, shR, shW, inB, outB, mem, disk, gc, times) }
    override def onJobStart(e: SparkListenerJobStart): Unit = synchronized { jobs += 1 }
    override def onStageCompleted(e: SparkListenerStageCompleted): Unit = synchronized { stages += 1 }
    override def onTaskEnd(e: SparkListenerTaskEnd): Unit = synchronized {
      val m = e.taskMetrics; tasks += 1
      if (m != null) {
        times :+= m.executorRunTime; gc += m.jvmGCTime
        if (m.inputMetrics  != null) inB += m.inputMetrics.bytesRead
        if (m.outputMetrics != null) outB+= m.outputMetrics.bytesWritten
        if (m.shuffleReadMetrics  != null) shR += m.shuffleReadMetrics.totalBytesRead
        if (m.shuffleWriteMetrics != null) shW += m.shuffleWriteMetrics.bytesWritten
        mem += m.memoryBytesSpilled; disk += m.diskBytesSpilled
      }
    }
  }

  // --------------------- 统计工具 ---------------------
  private def median(xs: Seq[Double]): Double = {
    if (xs.isEmpty) 0.0 else {
      val s = xs.sorted
      val n = s.length
      if (n % 2 == 1) s(n/2) else 0.5*(s(n/2-1)+s(n/2))
    }
  }
  private def stddev(xs: Seq[Double]): Double = {
    if (xs.isEmpty) 0.0 else {
      val m = xs.sum / xs.length
      math.sqrt(xs.map(x => (x-m)*(x-m)).sum / xs.length)
    }
  }
  private def qLong(p: Double, xs: Seq[Long]): Long = {
    if (xs.isEmpty) 0L
    else {
      val s = xs.sorted
      val n = s.length
      val pos = (n - 1) * p
      val idx = math.max(0, math.min(n - 1, math.round(pos).toInt))
      s(idx)
    }
  }
  private def percentile(xs: Seq[Double], p: Double): Double = {
    if (xs.isEmpty) 0.0 else {
      val s = xs.sorted
      val n = s.length
      val pos = (n - 1) * p
      val idx = math.max(0, math.min(n - 1, math.round(pos).toInt))
      s(idx)
    }
  }
  private def safeD(s:String): Double = Try(s.toDouble).getOrElse(Double.NaN)

  // --------------------- TOTAL PASS（Trials + Ablation） ---------------------
  private def runTotalPass(outFile: File,
                           cases: Seq[TestCase],
                           trials: Int,
                           ablation: Set[String]): Unit = {

    val header = "name,ablation,A_rows,A_cols,B_rows,B_cols,cells," +
      "RDD_ms,DF_ms,RDD_ms_min,RDD_ms_max,RDD_ms_std,DF_ms_min,DF_ms_max,DF_ms_std," +
      "rdd_jobs,rdd_stages,rdd_tasks,rdd_shuffle_read_bytes,rdd_shuffle_write_bytes,rdd_input_bytes,rdd_output_bytes," +
      "rdd_mem_spill_bytes,rdd_disk_spill_bytes,rdd_gc_ms,rdd_p50_ms,rdd_p90_ms,rdd_p99_ms,rdd_max_ms,rdd_hash," +
      "df_jobs,df_stages,df_tasks,df_shuffle_read_bytes,df_shuffle_write_bytes,df_input_bytes,df_output_bytes," +
      "df_mem_spill_bytes,df_disk_spill_bytes,df_gc_ms,df_p50_ms,df_p90_ms,df_p99_ms,df_max_ms,df_hash," +
      "preview_used\n"
    writeString(outFile, header, append=false)

    cases.foreach { tc =>
      val (aRows, aCols, bRows, bCols) = inferShapes(tc)
      val cells = aRows.toLong * (if (bCols==0) 1L else bCols.toLong)
      val preview = cells > MaxCellsForFullSave

      val rTimes = scala.collection.mutable.ArrayBuffer.empty[Double]
      val dTimes = scala.collection.mutable.ArrayBuffer.empty[Double]
      var rSnapLast: Metrics.Snap = null
      var dSnapLast: Metrics.Snap = null
      var rHash: String = ""
      var dHash: String = ""

      (1 to trials).foreach { _ =>
        // RDD total
        Metrics.install(Run.sc); Metrics.reset()
        val t0 = System.nanoTime()
        val rddOut = Run.Run(tc.left, tc.right)
        val rMs  = (System.nanoTime()-t0)/1e6
        rSnapLast  = Metrics.snap("RDD", rMs)
        rHash      = crc32Hex(if (preview) previewHead(rddOut) else rddOut)
        rTimes    += rMs

        // DF total
        Metrics.reset()
        val t1 = System.nanoTime()
        val dfOut =
          if (bRows == 1) dfSpmvToString(tc.left, tc.right, ablation)
          else dfSpmmToDenseString(tc.left, tc.right, aRows=aRows, bCols=bCols, previewLines = if (preview) Some(PreviewLinesForHuge) else None, ablation)
        val dfMs = (System.nanoTime()-t1)/1e6
        dSnapLast = Metrics.snap("DF", dfMs)
        dHash     = crc32Hex(dfOut)
        dTimes   += dfMs
      }

      val rMed = median(rTimes.toSeq); val dMed = median(dTimes.toSeq)
      val rStd = stddev(rTimes.toSeq); val dStd = stddev(dTimes.toSeq)

      val line = s"${tc.name},${ablationLabel(ablation)},$aRows,$aCols,$bRows,$bCols,$cells," +
        f"$rMed%.1f,$dMed%.1f,${rTimes.min}%.1f,${rTimes.max}%.1f,${rStd}%.1f,${dTimes.min}%.1f,${dTimes.max}%.1f,${dStd}%.1f," +
        s"${rSnapLast.jobs},${rSnapLast.stages},${rSnapLast.tasks},${rSnapLast.shR},${rSnapLast.shW},${rSnapLast.inB},${rSnapLast.outB},${rSnapLast.mem},${rSnapLast.disk},${rSnapLast.gc}," +
        s"${qLong(0.5,rSnapLast.times)},${qLong(0.9,rSnapLast.times)},${qLong(0.99,rSnapLast.times)},${(if (rSnapLast.times.isEmpty) 0L else rSnapLast.times.max)}," +
        s"$rHash," +
        s"${dSnapLast.jobs},${dSnapLast.stages},${dSnapLast.tasks},${dSnapLast.shR},${dSnapLast.shW},${dSnapLast.inB},${dSnapLast.outB},${dSnapLast.mem},${dSnapLast.disk},${dSnapLast.gc}," +
        s"${qLong(0.5,dSnapLast.times)},${qLong(0.9,dSnapLast.times)},${qLong(0.99,dSnapLast.times)},${(if (dSnapLast.times.isEmpty) 0L else dSnapLast.times.max)}," +
        s"$dHash,$preview\n"
      writeString(outFile, line, append=true)
    }
  }

  private def ablationLabel(ab: Set[String]): String =
    if (ab.isEmpty) "baseline" else ab.toList.sorted.mkString("+")

  // --------------------- KERNEL PASS（Trials + Ablation） ---------------------
  private def runKernelPass(outFile: File,
                            cases: Seq[TestCase],
                            trials: Int,
                            ablation: Set[String]): Unit = {
    writeString(outFile, "name,ablation,RDD_kernel_ms,DF_kernel_ms,RDD_kernel_min,RDD_kernel_max,DF_kernel_min,DF_kernel_max\n", append=false)
    cases.foreach { tc =>
      val (_, _, bRows, _) = inferShapes(tc)
      val rKs = scala.collection.mutable.ArrayBuffer.empty[Double]
      val dKs = scala.collection.mutable.ArrayBuffer.empty[Double]
      (1 to trials).foreach { _ =>
        rKs += Run.kernelOnlyMs(tc.left, tc.right)
        dKs += (if (bRows == 1) dfKernelMsSpmv(tc.left, tc.right, ablation) else dfKernelMsSpmm(tc.left, tc.right, ablation))
      }
      val line = s"${tc.name},${ablationLabel(ablation)}," +
        f"${median(rKs.toSeq)}%.1f,${median(dKs.toSeq)}%.1f,${rKs.min}%.1f,${rKs.max}%.1f,${dKs.min}%.1f,${dKs.max}%.1f\n"
      writeString(outFile, line, append=true)
    }
  }

  // --------------------- 合并 TOTAL + KERNEL 并派生速度提升 ---------------------
  private def mergeSummaries(totalFile: File, kernelFile: File, outFile: File): Unit = {
    // 读取 kernel → (name,ablation) -> (rK, dK)
    val kMap = scala.collection.mutable.HashMap.empty[(String,String),(String,String)]
    val srcK = scala.io.Source.fromFile(kernelFile); try {
      val it = srcK.getLines()
      if (it.hasNext) it.next()
      while (it.hasNext) {
        val arr = it.next().split(",", -1)
        if (arr.length >= 4) {
          val key = (arr(0), arr(1))
          val rK  = arr(2); val dK = arr(3)
          kMap.put(key, (rK, dK))
        }
      }
    } finally srcK.close()

    val inT  = new BufferedReader(new FileReader(totalFile))
    val out  = new BufferedWriter(new FileWriter(outFile))
    try {
      val header = inT.readLine()
      out.write(header + ",RDD_kernel_ms,DF_kernel_ms,speedup_total,speedup_kernel"); out.newLine()
      var line: String = null
      while ({ line = inT.readLine(); line != null }) {
        val arr  = line.split(",", -1)
        val name = arr.headOption.getOrElse("")
        val abl  = if (arr.length > 1) arr(1) else "baseline"
        val rMs  = if (arr.length > 7)  safeD(arr(7))  else Double.NaN
        val dMs  = if (arr.length > 8)  safeD(arr(8))  else Double.NaN
        val (rk, dk) = kMap.getOrElse((name, abl), ("",""))
        val rkD = safeD(rk); val dkD = safeD(dk)
        val spTotal  = if (rMs.isNaN || dMs.isNaN || rMs==0.0) "" else f"${dMs/rMs}%.3f"
        val spKernel = if (rkD.isNaN || dkD.isNaN || rkD==0.0) "" else f"${dkD/rkD}%.3f"
        out.write(line + s",$rk,$dk,$spTotal,$spKernel"); out.newLine()
      }
    } finally { inT.close(); out.close() }
  }

  // --------------------- 端到端：PageRank（不改算子） ---------------------
  private def runE2EPageRank(outRoot: File,
                             aPath: String,
                             steps: Int,
                             alpha: Double): Unit = {
    val outFile = new File(outRoot, "e2e_summary.csv")
    writeString(outFile, "matrix,A_rows,A_cols,steps,alpha,E2E_total_ms,step_kernel_ms_med,step_kernel_ms_p90,step_kernel_ms_max,final_residual\n", append=false)

    val (aRows, aCols) = shapeOf(aPath)
    require(aCols>0 && aRows>0, s"bad matrix shape: $aRows x $aCols")

    // 初始向量：1/N
    var v: Array[Double] = Array.fill(aCols)(1.0/aCols)
    val tmpVec = File.createTempFile("pr_vec_", ".csv")

    def writeRowVector(arr: Array[Double], file: File): Unit = {
      val bw = new BufferedWriter(new FileWriter(file))
      try bw.write(arr.mkString(",")) finally bw.close()
    }

    val stepKs = scala.collection.mutable.ArrayBuffer.empty[Double]
    val t0All  = System.nanoTime()
    var lastResidual = 0.0

    (1 to steps).foreach { _ =>
      writeRowVector(v, tmpVec) // row vector

      // kernel-only for this step (A·v)
      val kMs = Run.kernelOnlyMs(aPath, tmpVec.getAbsolutePath)
      stepKs += kMs

      // 真实结果（端到端）：A·v
      val resStr = Run.Run(aPath, tmpVec.getAbsolutePath)
      val resArr = resStr.split(",").map(_.trim).filter(_.nonEmpty).map(_.toDouble)

      // PageRank 更新：damping + L1 归一化
      val sumAbs = resArr.foldLeft(0.0)((acc,x) => acc + scala.math.abs(x))
      val norm   = if (sumAbs == 0.0) 1.0 else sumAbs
      val dangling = (1.0 - alpha) / aCols
      val newV = resArr.map(x => alpha * (x / norm) + dangling)

      // 残差（L1）
      lastResidual = newV.zip(v).foldLeft(0.0){ case (acc,(a,b)) => acc + scala.math.abs(a-b) }
      v = newV
    }

    val e2eMs = (System.nanoTime() - t0All) / 1e6
    val med   = median(stepKs.toSeq)
    val p90   = percentile(stepKs.toSeq, 0.90)
    val mx    = stepKs.max
    val line  = s"$aPath,$aRows,$aCols,$steps,$alpha,${f"$e2eMs%.1f"},${f"$med%.1f"},${f"$p90%.1f"},${f"$mx%.1f"},${f"$lastResidual%.6f"}\n"
    writeString(outFile, line, append=true)
    tmpVec.delete()
  }

  // --------------------- I/O & 形状 & 环境 ---------------------
  case class TestCase(name: String, left: String, right: String)
  private val smallCases = Seq(
    TestCase("small_spmv_3x3",          s"$dataRootProp/small/matrix_3x3_sparse.csv", s"$dataRootProp/small/vector_3.csv"),
    TestCase("small_spmm_3x3",          s"$dataRootProp/small/matrix_3x3_sparse.csv", s"$dataRootProp/small/matrix_3x3_dense.csv"),
    TestCase("small_identity_rect",     s"$dataRootProp/small/identity_5x5.csv",      s"$dataRootProp/small/rect_5x3.csv"),
    TestCase("small_zero_x_diag",       s"$dataRootProp/small/zero_4x4.csv",          s"$dataRootProp/small/diagonal_4x4.csv")
  )
  private val mediumCases = Seq(
    TestCase("med_100x100_1pct_x_100",  s"$dataRootProp/medium/matrix_100x100_1pct.csv",  s"$dataRootProp/medium/vector_100.csv"),
    TestCase("med_100x100_5pct_x_100",  s"$dataRootProp/medium/matrix_100x100_5pct.csv",  s"$dataRootProp/medium/vector_100.csv"),
    TestCase("med_100x100_10pct_x_100", s"$dataRootProp/medium/matrix_100x100_10pct.csv", s"$dataRootProp/medium/vector_100.csv"),
    TestCase("med_100x100_30pct_x_100", s"$dataRootProp/medium/matrix_100x100_30pct.csv", s"$dataRootProp/medium/vector_100.csv"),
    TestCase("med_rect_50x100_x_100",   s"$dataRootProp/medium/matrix_50x100_5pct.csv",   s"$dataRootProp/medium/vector_100.csv"),
    TestCase("med_rect_100x50_mul",     s"$dataRootProp/medium/matrix_100x50_5pct.csv",   s"$dataRootProp/medium/matrix_50x100_5pct.csv")
  )
  private val largeCases = Seq(
    TestCase("lg_1k_1pct_x_1k",         s"$dataRootProp/large/matrix_1k_1pct.csv",    s"$dataRootProp/large/vector_1000.csv"),
    TestCase("lg_5k_0_1pct_x_5k",       s"$dataRootProp/large/matrix_5k_0.1pct.csv",  s"$dataRootProp/large/vector_5000.csv"),
    TestCase("lg_10k_0_1pct_x_10k",     s"$dataRootProp/large/matrix_10k_0.1pct.csv", s"$dataRootProp/large/vector_10000.csv")
  )
  private val patternCases = Seq(
    TestCase("pat_tridiag_1000_x",      s"$dataRootProp/patterns/tridiagonal_1000.csv",    s"$dataRootProp/large/vector_1000.csv"),
    TestCase("pat_banded10_1000_x",     s"$dataRootProp/patterns/banded_1000_width10.csv", s"$dataRootProp/large/vector_1000.csv"),
    TestCase("pat_blockdiag_1000_x",    s"$dataRootProp/patterns/block_diagonal_1000.csv", s"$dataRootProp/large/vector_1000.csv"),
    TestCase("pat_powerlaw_1000_x",     s"$dataRootProp/patterns/power_law_1000.csv",      s"$dataRootProp/large/vector_1000.csv"),
    TestCase("pat_rowhot_1000_x",       s"$dataRootProp/patterns/row_hotspot_1000.csv",    s"$dataRootProp/large/vector_1000.csv")
  )
  private val benchCases = Seq(
    TestCase("bench_scale_1000",        s"$dataRootProp/benchmark/scale_1000x1000.csv",    s"$dataRootProp/benchmark/vector_1000.csv"),
    TestCase("bench_sparsity_2000_1pct",s"$dataRootProp/benchmark/sparsity_2000x2000_1_0pct.csv", s"$dataRootProp/benchmark/vector_2000.csv")
  )

  private def allCases(): Seq[TestCase] =
    smallCases ++ mediumCases ++ largeCases ++ patternCases ++ benchCases

  private def shapeOf(path: String): (Int,Int) = {
    val src = scala.io.Source.fromFile(path)
    try {
      val it = src.getLines()
      var rows = 0; var minC = Int.MaxValue
      while (it.hasNext) {
        val line = it.next().trim
        if (line.nonEmpty) {
          val c = line.split(",").count(_.trim.nonEmpty)
          rows += 1; if (c < minC) minC = c
        }
      }
      (rows, if (rows == 0) 0 else minC)
    } finally src.close()
  }

  private def inferShapes(tc: TestCase): (Int,Int,Int,Int) = {
    val (ar, ac) = shapeOf(tc.left); val (br, bc) = shapeOf(tc.right)
    (ar, ac, br, bc)
  }

  private def writeString(file: File, content: String, append: Boolean): Unit = {
    val parent = file.getParentFile; if (parent != null && !parent.exists()) parent.mkdirs()
    val bw = new BufferedWriter(new FileWriter(file, append))
    try bw.write(content) finally bw.close()
  }

  private def crc32Hex(s: String): String = {
    val c = new CRC32; val bytes = s.getBytes("UTF-8"); c.update(bytes, 0, bytes.length)
    f"${c.getValue}%08x"
  }

  private def previewHead(s: String): String =
    s.split("\\r?\\n").take(PreviewLinesForHuge).mkString("\n")

  private def timeStamp(): String = {
    val z = java.time.ZonedDateTime.now()
    f"${z.getYear}%04d${z.getMonthValue}%02d${z.getDayOfMonth}%02d-${z.getHour}%02d${z.getMinute}%02d${z.getSecond}%02d"
  }

  private def writeRunInfo(base: File, mode: String, trials: Int, ablation: Set[String]): Unit = {
    val sp = spark()

    // 用 getOption，避免 typed 解析；取不到时写 "unset"
    val shufflePart = sp.conf.getOption("spark.sql.shuffle.partitions").getOrElse("unset")
    val aqeEnabled  = sp.conf.getOption("spark.sql.adaptive.enabled").getOrElse("unset")
    val brdThr      = sp.conf.getOption("spark.sql.autoBroadcastJoinThreshold").getOrElse("unset")

    val sb = new StringBuilder
    sb.append(s"Timestamp: ${timeStamp()}\n")
    sb.append(s"Mode: $mode\n")
    sb.append(s"Trials: $trials\n")
    sb.append(s"Ablation: ${ablationLabel(ablation)}\n")
    sb.append(s"Spark version: ${org.apache.spark.SPARK_VERSION}\n")
    sb.append(s"Spark master : ${sp.sparkContext.master}\n")
    sb.append(s"Default parallelism: ${sp.sparkContext.defaultParallelism}\n")
    sb.append(s"JVM: ${System.getProperty("java.version")} ${System.getProperty("java.vendor")}\n")
    sb.append(s"OS : ${System.getProperty("os.name")} ${System.getProperty("os.version")}\n")
    sb.append(s"Conf: shuffle.partitions=$shufflePart, AQE=$aqeEnabled, broadcastThr=$brdThr\n")

    writeString(new File(base,"run_info.txt"), sb.toString(), append=false)
  }


  // --------------------- CLI 解析 ---------------------
  private case class Args(mode:String="both", trials:Int=1, ablation:Set[String]=Set.empty,
                          e2eMatrix: Option[String]=None, steps:Int=20, alpha:Double=0.85)
  private def parseArgs(argv: Array[String]): Args = {
    var a = Args()
    argv.foreach {
      case s if s.startsWith("--mode=")   => a = a.copy(mode = s.split("=",2)(1).toLowerCase)
      case s if s.startsWith("--trials=") => a = a.copy(trials = Try(s.split("=",2)(1).toInt).getOrElse(1).max(1))
      case s if s.startsWith("--ablation=") =>
        val set = s.split("=",2)(1).split(",").map(_.trim.toLowerCase).filter(_.nonEmpty).toSet
        a = a.copy(ablation = set)
      case s if s.startsWith("--e2e_matrix=") =>
        a = a.copy(e2eMatrix = Some(s.split("=",2)(1)))
      case s if s.startsWith("--steps=") =>
        a = a.copy(steps = Try(s.split("=",2)(1).toInt).getOrElse(20).max(1))
      case s if s.startsWith("--alpha=") =>
        a = a.copy(alpha = Try(s.split("=",2)(1).toDouble).getOrElse(0.85))
      case _ =>
    }
    a
  }

  // --------------------- 主入口 ---------------------
  def main(argv: Array[String]): Unit = {
    val args = parseArgs(argv)
    val outRoot = new File(s"runs/${timeStamp()}"); outRoot.mkdirs()

    writeRunInfo(outRoot, args.mode, args.trials, args.ablation)

    args.mode match {
      case "total" =>
        val fT = new File(outRoot, "summary_total.csv")
        runTotalPass(fT, allCases(), args.trials, args.ablation)
        println(s"[DONE] total -> ${fT.getAbsolutePath}")

      case "kernel" =>
        val fK = new File(outRoot, "summary_kernel.csv")
        runKernelPass(fK, allCases(), args.trials, args.ablation)
        println(s"[DONE] kernel -> ${fK.getAbsolutePath}")

      case "both" =>
        val fT = new File(outRoot, "summary_total.csv")
        runTotalPass(fT, allCases(), args.trials, args.ablation)
        // 分轮运行，避免互相干扰
        Thread.sleep(2000)
        val fK = new File(outRoot, "summary_kernel.csv")
        runKernelPass(fK, allCases(), args.trials, args.ablation)
        val fOut = new File(outRoot, "summary.csv")
        mergeSummaries(fT, fK, fOut)
        println(s"[DONE] merged -> ${fOut.getAbsolutePath}")

      case "e2e" =>
        val m = args.e2eMatrix.getOrElse {
          System.err.println("ERROR: --mode=e2e 需要提供 --e2e_matrix=<path>"); sys.exit(1)
        }
        runE2EPageRank(outRoot, m, args.steps, args.alpha)
        println(s"[DONE] e2e -> ${new File(outRoot,"e2e_summary.csv").getAbsolutePath}")

      case other =>
        System.err.println(s"Unknown --mode: $other (expect total|kernel|both|e2e)")
    }
  }
}





//import java.io._
//import java.time.format.DateTimeFormatter
//import java.time.{Instant, ZoneId}
//import java.util.zip.CRC32
//import scala.util.Try
//
//import org.apache.spark.SparkContext
//import org.apache.spark.scheduler._
//import org.apache.spark.sql.{SparkSession, Dataset}
//import org.apache.spark.sql.functions._
//import org.apache.spark.HashPartitioner
//
///** 批量测试入口：一次执行，自动跑完 RDD vs DataFrame，对比并落盘（强汇总版） */
//object PDSSBatchBench {
//
//  // ============ 配置 ============
//
//  // 是否为每个用例写单独的结果/指标文件（默认 false：只汇总到 summary.csv）
//  val WritePerCaseArtifacts: Boolean = false
//
//  // 结果大到多少单元时只保存预览（否则写完整字符串）
//  val MaxCellsForFullSave: Long = 2_000_000L
//  val PreviewLinesForHuge   : Int  = 100
//
//  // 数据根（可用 -Dpdss.data.dir=... 覆盖）
//  private val dataRootProp = sys.props.getOrElse("pdss.data.dir", "test_data")
//
//  // ============ Runner（RDD 实现） ============
//  val Run = new Runner
//
//  // ============ DF 对照（与前端一致） ============
//  case class Entry(i: Int, j: Int, v: Double)
//  case class Vec(k: Int, x: Double)
//
//  private def spark(): SparkSession =
//    SparkSession.builder().config(Run.sc.getConf).getOrCreate()
//
//  private def dfReadDenseCsvAsCOO(path: String): Dataset[Entry] = {
//    val sp = spark(); import sp.implicits._
//    val lines = Run.sc.textFile(path)
//    val coo = lines.zipWithIndex().flatMap { case (line, rowIdx) =>
//      val toks = line.split(",").map(_.trim).filter(_.nonEmpty)
//      toks.zipWithIndex.flatMap { case (s, colIdx) =>
//        Try(s.toDouble).toOption match {
//          case Some(d) if d != 0.0 => Some(Entry(rowIdx.toInt, colIdx, d))
//          case _ => None
//        }
//      }
//    }
//    sp.createDataset(coo)
//  }
//  private def dfReadVectorRow(path: String): Dataset[Vec] = {
//    val sp = spark(); import sp.implicits._
//    val first = Run.sc.textFile(path).filter(_.trim.nonEmpty).first()
//    val toks  = first.split(",").map(_.trim).filter(_.nonEmpty)
//    val vec   = toks.zipWithIndex.flatMap { case (s, idx) => Try(s.toDouble).toOption.map(d => Vec(idx, d)) }
//    sp.createDataset(vec.toSeq)
//  }
//  private def dfSpmvToString(pathA: String, pathX: String): String = {
//    val sp = spark(); import sp.implicits._
//    val A = dfReadDenseCsvAsCOO(pathA)
//    val x = dfReadVectorRow(pathX)
//    val Aj = A.withColumnRenamed("j","k").withColumnRenamed("v","aik")
//    val X  = x.toDF("k","x")
//    val dfY = Aj.join(X, Seq("k"))
//      .select(col("i"), (col("aik")*col("x")).as("prod"))
//      .groupBy("i").agg(sum("prod").as("y"))
//      .orderBy("i").select(col("y"))
//    dfY.rdd.map(_.getDouble(0).toString)
//      .coalesce(1, shuffle = true).mapPartitions(it => Iterator.single(it.mkString(",")))
//      .first()
//  }
//  private def dfSpmmToDenseString(pathA: String, pathB: String, aRows: Int, bCols: Int,
//                                  previewLines: Option[Int] = None): String = {
//    val sp = spark(); import sp.implicits._
//    val A = dfReadDenseCsvAsCOO(pathA)
//    val B = dfReadDenseCsvAsCOO(pathB)
//    val Ak = A.withColumnRenamed("j","k").withColumnRenamed("v","aik")
//    val Bk = B.withColumnRenamed("i","k").withColumnRenamed("v","kbj")
//    val dfC = Ak.join(Bk, Seq("k"))
//      .select(col("i"), col("j"), (col("aik")*col("kbj")).as("prod"))
//      .groupBy("i","j").agg(sum("prod").as("cij"))
//
//    val sc = Run.sc
//    val P  = sc.defaultParallelism
//    val part = new HashPartitioner(P)
//    val rows: org.apache.spark.rdd.RDD[(Int,(Int,Double))] =
//      dfC.rdd.map(r => (r.getInt(0), (r.getInt(1), r.getDouble(2))))
//    val rowMap = rows.partitionBy(part).combineByKey[scala.collection.mutable.Map[Int,Double]](
//      (cv: (Int,Double)) => { val m = scala.collection.mutable.Map[Int,Double](); m.update(cv._1, cv._2); m },
//      (m:  scala.collection.mutable.Map[Int,Double], cv: (Int,Double)) => { m.update(cv._1, cv._2); m },
//      (m1: scala.collection.mutable.Map[Int,Double], m2: scala.collection.mutable.Map[Int,Double]) => {
//        if (m2.size > m1.size) { m2 ++= m1; m2 } else { m1 ++= m2; m1 }
//      }
//    )
//    val allRows = sc.parallelize(0 until aRows, P).map(i => (i, ()))
//    val denseLines = allRows.leftOuterJoin(rowMap)
//      .sortByKey(numPartitions = P)
//      .map { case (_, (_, mOpt)) =>
//        val m = mOpt.getOrElse(scala.collection.mutable.Map.empty[Int,Double])
//        val sb = new StringBuilder
//        var j = 0
//        while (j < bCols) { if (j > 0) sb.append(", "); sb.append(m.getOrElse(j, 0.0)); j += 1 }
//        sb.toString
//      }
//    previewLines match {
//      case Some(n) => denseLines.take(n).mkString("\n")
//      case None    => denseLines.coalesce(1, shuffle = true).mapPartitions(it => Iterator.single(it.mkString("\n"))).first()
//    }
//  }
//
//  // ============ 指标采集（与前端同口径） ============
//  object Metrics extends SparkListener {
//    case class Snapshot(label: String,
//                        wallMs: Double,
//                        jobs: Int, stages: Int, tasks: Int,
//                        shuffleRead: Long, shuffleWrite: Long,
//                        inputBytes: Long, outputBytes: Long,
//                        memSpill: Long, diskSpill: Long,
//                        gcTimeMs: Long,
//                        taskTimesMs: Vector[Long])
//    private var installed=false
//    private var jobs=0; private var stages=0; private var tasks=0
//    private var shuffleRead=0L; private var shuffleWrite=0L
//    private var inputBytes=0L; private var outputBytes=0L
//    private var memSpill=0L; private var diskSpill=0L
//    private var gcTime=0L
//    private var taskTimes=Vector.empty[Long]
//    def install(sc: SparkContext): Unit = synchronized { if (!installed) { sc.addSparkListener(this); installed=true } }
//    def reset(): Unit = synchronized {
//      jobs=0; stages=0; tasks=0; shuffleRead=0L; shuffleWrite=0L
//      inputBytes=0L; outputBytes=0L; memSpill=0L; diskSpill=0L; gcTime=0L; taskTimes=Vector.empty
//    }
//    def snapshot(label:String, wallMs:Double) = synchronized {
//      Snapshot(label, wallMs, jobs, stages, tasks, shuffleRead, shuffleWrite, inputBytes, outputBytes, memSpill, diskSpill, gcTime, taskTimes)
//    }
//    override def onJobStart(e: SparkListenerJobStart): Unit = synchronized { jobs += 1 }
//    override def onStageCompleted(e: SparkListenerStageCompleted): Unit = synchronized { stages += 1 }
//    override def onTaskEnd(e: SparkListenerTaskEnd): Unit = synchronized {
//      val m = e.taskMetrics; tasks += 1
//      if (m != null) {
//        taskTimes :+= m.executorRunTime
//        gcTime += m.jvmGCTime
//        if (m.inputMetrics != null) inputBytes += m.inputMetrics.bytesRead
//        if (m.outputMetrics != null) outputBytes += m.outputMetrics.bytesWritten
//        if (m.shuffleReadMetrics != null) shuffleRead += m.shuffleReadMetrics.totalBytesRead
//        if (m.shuffleWriteMetrics != null) shuffleWrite += m.shuffleWriteMetrics.bytesWritten
//        memSpill += m.memoryBytesSpilled; diskSpill += m.diskBytesSpilled
//      }
//    }
//  }
//
//  // ============ 汇总工具 ============
//  private def quantile(p: Double, xs: Vector[Long]): Long = {
//    if (xs.isEmpty) 0L
//    else {
//      val s   = xs.sorted
//      val n   = s.length
//      val pos = (n - 1) * p
//      val idx = math.max(0, math.min(n - 1, math.round(pos).toInt))
//      s(idx)
//    }
//  }
//  private def crc32Hex(s: String): String = {
//    val c = new CRC32; val bytes = s.getBytes("UTF-8"); c.update(bytes, 0, bytes.length)
//    f"${c.getValue}%08x"
//  }
//  private def nowTag(): String = {
//    val z = Instant.now().atZone(ZoneId.systemDefault())
//    z.format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"))
//  }
//  private def ensureDir(d: File): Unit = { if (d != null && !d.exists()) d.mkdirs() }
//  private def writeString(file: File, content: String, append:Boolean=false): Unit = {
//    ensureDir(file.getParentFile)
//    val bw = new BufferedWriter(new FileWriter(file, append))
//    try bw.write(content) finally bw.close()
//  }
//  private def preview(s: String, n: Int): String = s.split("\\r?\\n").take(n).mkString("\n")
//  private val SummaryHeader: String =
//    "name,A_rows,A_cols,B_rows,B_cols,cells," +
//      "RDD_ms,DF_ms," +
//      "rdd_jobs,rdd_stages,rdd_tasks,rdd_shuffle_read_bytes,rdd_shuffle_write_bytes,rdd_input_bytes,rdd_output_bytes," +
//      "rdd_mem_spill_bytes,rdd_disk_spill_bytes,rdd_gc_ms,rdd_p50_ms,rdd_p90_ms,rdd_p99_ms,rdd_max_ms,rdd_hash," +
//      "df_jobs,df_stages,df_tasks,df_shuffle_read_bytes,df_shuffle_write_bytes,df_input_bytes,df_output_bytes," +
//      "df_mem_spill_bytes,df_disk_spill_bytes,df_gc_ms,df_p50_ms,df_p90_ms,df_p99_ms,df_max_ms,df_hash," +
//      "preview_used\n"
//
//  private def toSummaryLine(name:String, a:(Int,Int), b:(Int,Int),
//                            rdd: Metrics.Snapshot, df: Metrics.Snapshot,
//                            cells: Long, rddHash: String, dfHash: String,
//                            previewUsed: Boolean): String = {
//    val (r50,r90,r99,rMax) = (quantile(0.5,rdd.taskTimesMs), quantile(0.9,rdd.taskTimesMs),
//      quantile(0.99,rdd.taskTimesMs), if (rdd.taskTimesMs.isEmpty) 0L else rdd.taskTimesMs.max)
//    val (d50,d90,d99,dMax) = (quantile(0.5,df.taskTimesMs),  quantile(0.9,df.taskTimesMs),
//      quantile(0.99,df.taskTimesMs),  if (df.taskTimesMs.isEmpty) 0L else df.taskTimesMs.max)
//
//    Seq(
//      name, a._1, a._2, b._1, b._2, cells,
//      f"${rdd.wallMs}%.1f", f"${df.wallMs}%.1f",
//      rdd.jobs, rdd.stages, rdd.tasks, rdd.shuffleRead, rdd.shuffleWrite, rdd.inputBytes, rdd.outputBytes,
//      rdd.memSpill, rdd.diskSpill, rdd.gcTimeMs, r50, r90, r99, rMax, rddHash,
//      df.jobs, df.stages, df.tasks, df.shuffleRead, df.shuffleWrite, df.inputBytes, df.outputBytes,
//      df.memSpill, df.diskSpill, df.gcTimeMs, d50, d90, d99, dMax, dfHash,
//      previewUsed
//    ).mkString("", ",", "\n")
//  }
//
//  // ============ 用例 ============
//  case class TestCase(name: String, left: String, right: String)
//  val smallCases = Seq(
//    TestCase("small_spmv_3x3",          s"$dataRootProp/small/matrix_3x3_sparse.csv", s"$dataRootProp/small/vector_3.csv"),
//    TestCase("small_spmm_3x3",          s"$dataRootProp/small/matrix_3x3_sparse.csv", s"$dataRootProp/small/matrix_3x3_dense.csv"),
//    TestCase("small_identity_rect",     s"$dataRootProp/small/identity_5x5.csv",      s"$dataRootProp/small/rect_5x3.csv"),
//    TestCase("small_zero_x_diag",       s"$dataRootProp/small/zero_4x4.csv",          s"$dataRootProp/small/diagonal_4x4.csv")
//  )
//  val mediumCases = Seq(
//    TestCase("med_100x100_1pct_x_100",  s"$dataRootProp/medium/matrix_100x100_1pct.csv",  s"$dataRootProp/medium/vector_100.csv"),
//    TestCase("med_100x100_5pct_x_100",  s"$dataRootProp/medium/matrix_100x100_5pct.csv",  s"$dataRootProp/medium/vector_100.csv"),
//    TestCase("med_100x100_10pct_x_100", s"$dataRootProp/medium/matrix_100x100_10pct.csv", s"$dataRootProp/medium/vector_100.csv"),
//    TestCase("med_100x100_30pct_x_100", s"$dataRootProp/medium/matrix_100x100_30pct.csv", s"$dataRootProp/medium/vector_100.csv"),
//    TestCase("med_rect_50x100_x_100",   s"$dataRootProp/medium/matrix_50x100_5pct.csv",   s"$dataRootProp/medium/vector_100.csv"),
//    TestCase("med_rect_100x50_mul",     s"$dataRootProp/medium/matrix_100x50_5pct.csv",   s"$dataRootProp/medium/matrix_50x100_5pct.csv")
//  )
//  val largeCases = Seq(
//    TestCase("lg_1k_1pct_x_1k",         s"$dataRootProp/large/matrix_1k_1pct.csv",    s"$dataRootProp/large/vector_1000.csv"),
//    TestCase("lg_5k_0_1pct_x_5k",       s"$dataRootProp/large/matrix_5k_0.1pct.csv",  s"$dataRootProp/large/vector_5000.csv"),
//    TestCase("lg_10k_0_1pct_x_10k",     s"$dataRootProp/large/matrix_10k_0.1pct.csv", s"$dataRootProp/large/vector_10000.csv")
//  )
//  val patternCases = Seq(
//    TestCase("pat_tridiag_1000_x",      s"$dataRootProp/patterns/tridiagonal_1000.csv",    s"$dataRootProp/large/vector_1000.csv"),
//    TestCase("pat_banded10_1000_x",     s"$dataRootProp/patterns/banded_1000_width10.csv", s"$dataRootProp/large/vector_1000.csv"),
//    TestCase("pat_blockdiag_1000_x",    s"$dataRootProp/patterns/block_diagonal_1000.csv", s"$dataRootProp/large/vector_1000.csv"),
//    TestCase("pat_powerlaw_1000_x",     s"$dataRootProp/patterns/power_law_1000.csv",      s"$dataRootProp/large/vector_1000.csv"),
//    TestCase("pat_rowhot_1000_x",       s"$dataRootProp/patterns/row_hotspot_1000.csv",    s"$dataRootProp/large/vector_1000.csv")
//  )
//  val benchCases = Seq(
//    TestCase("bench_scale_1000",        s"$dataRootProp/benchmark/scale_1000x1000.csv",    s"$dataRootProp/benchmark/vector_1000.csv"),
//    TestCase("bench_sparsity_2000_1pct",s"$dataRootProp/benchmark/sparsity_2000x2000_1_0pct.csv", s"$dataRootProp/benchmark/vector_2000.csv")
//  )
//
//  // ============ 主流程 ============
//
//  def main(args: Array[String]): Unit = {
//    val outRoot = new File(s"runs/${nowTag()}"); ensureDir(outRoot)
//    writeString(new File(outRoot, "summary.csv"), SummaryHeader, append = false)
//
//    val cases = smallCases ++ mediumCases ++ largeCases ++ patternCases ++ benchCases
//    cases.foreach(tc => runCase(tc, outRoot))
//
//    println(s"\n✅ ALL DONE. Summary: ${new File(outRoot, "summary.csv").getAbsolutePath}")
//  }
//
//  // ============ 单用例执行 ============
//
//  private def runCase(tc: TestCase, outRoot: File): Unit = {
//    println(s"\n== Running: ${tc.name} ==")
//
//    // 形状推断
//    def inferShape(path: String): (Int, Int) = {
//      val src = scala.io.Source.fromFile(path)
//      try {
//        val it = src.getLines()
//        var rows = 0; var minC = Int.MaxValue
//        while (it.hasNext) {
//          val line = it.next().trim
//          if (line.nonEmpty) {
//            val c = line.split(",").count(_.trim.nonEmpty)
//            rows += 1; if (c < minC) minC = c
//          }
//        }
//        (rows, if (rows == 0) 0 else minC)
//      } finally src.close()
//    }
//    val aShape = inferShape(tc.left)
//    val bShape = inferShape(tc.right)
//    val totalCells = aShape._1.toLong * (if (bShape._2 == 0) 1L else bShape._2.toLong)
//    val previewUsed = totalCells > MaxCellsForFullSave
//
//    // RDD
//    Metrics.install(Run.sc); Metrics.reset()
//    val t0 = System.nanoTime()
//    val rddResult = Run.Run(tc.left, tc.right)
//    val rddSnap   = Metrics.snapshot("RDD", (System.nanoTime()-t0)/1e6)
//    val rddHash   = crc32Hex(if (previewUsed) preview(rddResult, PreviewLinesForHuge) else rddResult)
//
//    // DF（行向量走 SpMV，否则 SpMM 稠密化）
//    Metrics.reset()
//    val t1 = System.nanoTime()
//    val dfResult =
//      if (bShape._1 == 1) dfSpmvToString(tc.left, tc.right)
//      else dfSpmmToDenseString(tc.left, tc.right, aRows=aShape._1, bCols=bShape._2, previewLines = if (previewUsed) Some(PreviewLinesForHuge) else None)
//    val dfSnap   = Metrics.snapshot("DataFrame", (System.nanoTime()-t1)/1e6)
//    val dfHash   = crc32Hex(dfResult)
//
//    // 汇总写一行
//    appendSummary(outRoot, tc.name, aShape, bShape, totalCells, rddSnap, dfSnap, rddHash, dfHash, previewUsed)
//
//    // 可选：每用例的独立文件
//    if (WritePerCaseArtifacts) {
//      val caseDir = new File(outRoot, tc.name); ensureDir(caseDir)
//      val rddOut = new File(caseDir, if (previewUsed) "result_rdd_preview.csv" else "result_rdd.csv")
//      val dfOut  = new File(caseDir, if (previewUsed) "result_df_preview.csv"  else "result_df.csv")
//      writeString(rddOut, if (previewUsed) preview(rddResult, PreviewLinesForHuge) else rddResult)
//      writeString(dfOut,  dfResult)
//      writeString(new File(caseDir, "metrics_rdd.txt"), prettyMetrics(rddSnap, aShape, bShape))
//      writeString(new File(caseDir, "metrics_df.txt"),  prettyMetrics(dfSnap,  aShape, bShape))
//    }
//
//    println(f"Done: RDD ${rddSnap.wallMs}%.1f ms | DF ${dfSnap.wallMs}%.1f ms")
//  }
//
//  private def prettyMetrics(s: Metrics.Snapshot, a:(Int,Int), b:(Int,Int)): String = {
//    val p50=quantile(0.50,s.taskTimesMs); val p90=quantile(0.90,s.taskTimesMs); val p99=quantile(0.99,s.taskTimesMs); val mx= if (s.taskTimesMs.isEmpty) 0L else s.taskTimesMs.max
//    def hb(x:Long)={ val kb=1024.0; val mb=kb*1024; val gb=mb*1024; if(x>=gb) f"${x/gb}%.2f GB" else if(x>=mb) f"${x/mb}%.2f MB" else if(x>=kb) f"${x/kb}%.2f KB" else s"$x B" }
//    val br = if (b._1==0 && b._2==0) s"x=|${a._2}|" else s"B=${b._1}x${b._2}"
//    s"""|[${s.label}]  A=${a._1}x${a._2} , $br
//        |Wall time   : ${"%.1f".format(s.wallMs)} ms
//        |Jobs/Stages/Tasks : ${s.jobs}/${s.stages}/${s.tasks}
//        |Shuffle      : read ${hb(s.shuffleRead)} , write ${hb(s.shuffleWrite)}
//        |IO           : input ${hb(s.inputBytes)} , output ${hb(s.outputBytes)}
//        |Spill        : memory ${hb(s.memSpill)} , disk ${hb(s.diskSpill)}
//        |JVM GC       : ${s.gcTimeMs} ms
//        |Task runtime : p50=$p50 ms , p90=$p90 ms , p99=$p99 ms , max=$mx ms
//        |""".stripMargin
//  }
//
//  private def appendSummary(root: File,
//                            name: String,
//                            a: (Int,Int), b:(Int,Int), cells: Long,
//                            rddSnap: Metrics.Snapshot, dfSnap: Metrics.Snapshot,
//                            rddHash: String, dfHash: String, previewUsed:Boolean): Unit = {
//    val f = new File(root, "summary.csv")
//    val line = toSummaryLine(name, a, b, rddSnap, dfSnap, cells, rddHash, dfHash, previewUsed)
//    writeString(f, line, append = true)
//  }
//}


