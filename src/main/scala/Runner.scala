import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.util.Try
import scala.collection.mutable

// ----------- 仅函数工具，不捕获 Runner.this -----------
object RunnerFuncs extends Serializable {

  /** 只保留纯数字的 CSV 行（过滤掉 GUI 提示/注释等） */
  def isNumericLine(line: String): Boolean = {
    val it = line.split(",").iterator
    var any = false; var ok = true
    while (ok && it.hasNext) {
      val s = it.next().trim
      if (s.nonEmpty) {
        if (Try(s.toDouble).isFailure) ok = false else any = true
      }
    }
    any && ok
  }

  /** 稀疏性判定（忽略非数字 token） */
  def isSparse(matrix: RDD[String]): Boolean = {
    val (nnz, total) = matrix.mapPartitions { it =>
      var nonZero = 0L; var count = 0L
      it.foreach { line =>
        val arr = line.split(","); var i = 0
        while (i < arr.length) {
          val s = arr(i).trim
          if (s.nonEmpty) {
            Try(s.toDouble).toOption.foreach { d =>
              count += 1; if (d != 0.0) nonZero += 1
            }
          }
          i += 1
        }
      }
      Iterator.single((nonZero, count))
    }.reduce { case ((a1, a2), (b1, b2)) => (a1 + b1, a2 + b2) }

    val rate = if (total == 0) 0.0 else nnz.toDouble / total.toDouble
    println(f"[isS] nonzero rate = $rate%.4f , total=$total")
    rate < 0.5
  }

  /** 向量 → 单行 CSV（保证全局顺序） */
  def vectorToCsvLine(vec: RDD[Double]): String = {
    val ordered: RDD[String] =
      vec.zipWithIndex()
        .map{ case (v,i) => (i, v) }
        .sortByKey()
        .map{ _._2.toString }

    ordered.coalesce(1, shuffle = true)
      .mapPartitions(it => Iterator.single(it.mkString(",")))
      .first()
  }

  /** COO → 稠密矩阵字符串（保证行的全局顺序） */
  def cooToDenseString(coo: RDD[(Int, Int, Double)],
                       shape: (Int, Int))
                      (implicit sc: SparkContext): String = {
    val (numRows, numCols) = shape
    val P    = sc.defaultParallelism
    val part = new HashPartitioner(P)

    val rows: RDD[(Int, (Int, Double))] =
      coo.map { case (i, j, v) => (i, (j, v)) }

    val rowMap: RDD[(Int, mutable.Map[Int, Double])] =
      rows.partitionBy(part).combineByKey[mutable.Map[Int, Double]](
        (cv: (Int, Double)) => { val m = mutable.Map[Int, Double](); m.update(cv._1, cv._2); m },
        (m: mutable.Map[Int, Double], cv: (Int, Double)) => { m.update(cv._1, cv._2); m },
        (m1: mutable.Map[Int, Double], m2: mutable.Map[Int, Double]) => {
          if (m2.size > m1.size) { m2 ++= m1; m2 } else { m1 ++= m2; m1 }
        }
      )

    val allRows: RDD[(Int, Unit)] =
      sc.parallelize(0 until numRows, P).map(i => (i, ()))

    val denseLinesSorted: RDD[String] =
      allRows.leftOuterJoin(rowMap)
        .sortByKey(numPartitions = P)
        .map { case (_, (_, mOpt)) =>
          val m = mOpt.getOrElse(mutable.Map.empty[Int, Double])
          val sb = new StringBuilder
          var j = 0
          while (j < numCols) {
            if (j > 0) sb.append(", ")
            sb.append(m.getOrElse(j, 0.0))
            j += 1
          }
          sb.toString
        }

    denseLinesSorted.coalesce(1, shuffle = true)
      .mapPartitions(it => Iterator.single(it.mkString("\n")))
      .first()
  }
}

// ------------------------------ Runner 本体 ------------------------------
class Runner {

  // Windows 兜底（winutils）
  private val isWin = System.getProperty("os.name").toLowerCase.contains("win")
  if (isWin) {
    val hh = sys.env.getOrElse("HADOOP_HOME", "C:\\hadoop")
    System.setProperty("hadoop.home.dir", hh)
  }

  // Spark
  private val conf: SparkConf = new SparkConf()
    .setAppName("CW1")
    .setMaster("local[*]")
    .set("spark.ui.enabled", "false")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  implicit val sc: SparkContext = new SparkContext(conf)
  sc.setLogLevel("WARN")

  // 组件（放在 sc 之后）
  val Cal = new Calculator
  val Con = new Converter

  // 暴露上一次 RDD kernel-only 时间（毫秒）
  @volatile var lastRDDKernelMs: Double = 0.0

  /** 仅计算 RDD "kernel-only"（join+reduce）时间；不 densify、不返回结果 */
  def kernelOnlyMs(address1: String, address2: String): Double = {
    import RunnerFuncs._
    val P    = sc.defaultParallelism
    val part = new HashPartitioner(P)

    val SMraw   = sc.textFile(address1)
    val fileraw = sc.textFile(address2)

    val SM   = SMraw.filter(RunnerFuncs.isNumericLine _)
    val file = fileraw.filter(RunnerFuncs.isNumericLine _)

    // <<< 新增：左矩阵必须稀疏（非零率 < 0.5）
    if (!RunnerFuncs.isSparse(SM)) {
      throw new IllegalArgumentException("左侧矩阵不是稀疏矩阵（非零率≥50%）。请提供稀疏输入。")
    }

    val numRowsRight = file.count().toInt
    if (numRowsRight == 0) { lastRDDKernelMs = 0.0; return 0.0 }
    val numColsRight = file.first().split(",").length

    if (numRowsRight == 1) {
      if (isSparse(file)) {
        // 稀疏向量
        val (a_byRow, _)        = Con.SMToJoinableByRow(SM)
        val (svIdx, svVal, _)   = Con.ReadSV(file)
        val aCached = a_byRow.persist(); aCached.count()
        val svIdxC  = svIdx.persist();  svIdxC.count()
        val svValC  = svVal.persist();  svValC.count()

        val byCol = aCached.map{ case (r,(c,a)) => (c,(r,a)) }.partitionBy(part)
        val svKV  = svIdxC.zip(svValC).map{ case (i,v) => (i,v) }.partitionBy(part)

        val tKer = System.nanoTime()
        val summed = byCol.join(svKV).map{ case (_ , ((r,a),x)) => (r, a*x) }.reduceByKey(part, _ + _).persist()
        summed.count()
        lastRDDKernelMs = (System.nanoTime() - tKer) / 1e6

        summed.unpersist(false); aCached.unpersist(false); svIdxC.unpersist(false); svValC.unpersist(false)

      } else {
        // 稠密向量（单行）
        val (a_byRow, _)  = Con.SMToJoinableByRow(SM)
        val (dv, _)       = Con.ReadDV(file)
        val aCached = a_byRow.persist(); aCached.count()
        val dvC     = dv.persist();      dvC.count()

        val byCol = aCached.map{ case (r,(c,a)) => (c,(r,a)) }.partitionBy(part)
        val vecKV = dvC.zipWithIndex().map{ case (v,i) => (i.toInt, v) }.partitionBy(part)

        val tKer = System.nanoTime()
        val summed = byCol.join(vecKV).map{ case (_ , ((r,a),x)) => (r, a*x) }.reduceByKey(part, _ + _).persist()
        summed.count()
        lastRDDKernelMs = (System.nanoTime() - tKer) / 1e6

        summed.unpersist(false); aCached.unpersist(false); dvC.unpersist(false)
      }
    } else {
      if (isSparse(file)) {
        val (a_byRow, _) = Con.SMToJoinableByRow(SM)
        val (b_byCol, _) = Con.SMToJoinableByCol(file)
        val aC = a_byRow.persist(); aC.count()
        val bC = b_byCol.persist(); bC.count()

        val aByK = aC.map{ case (i,(k,a)) => (k,(i,a)) }.partitionBy(part)
        val bByK = bC.map{ case (j,(k,b)) => (k,(j,b)) }.partitionBy(part)

        val tKer = System.nanoTime()
        val coo  = aByK.join(bByK).map{ case (_ , ((i,a),(j,b))) => ((i,j), a*b) }.reduceByKey(part, _ + _).persist()
        coo.count()
        lastRDDKernelMs = (System.nanoTime() - tKer) / 1e6

        coo.unpersist(false); aC.unpersist(false); bC.unpersist(false)

      } else {
        val (a_byRow, _) = Con.SMToJoinableByRow(SM)
        val (bDM, _)     = Con.ReadDM(file)
        val aC = a_byRow.persist(); aC.count()
        val bC = bDM.persist();    bC.count()

        val aByK = aC.map{ case (i,(k,a)) => (k,(i,a)) }.partitionBy(part)
        val bByK = bC.zipWithIndex().flatMap { case (row, kL) =>
          val k = kL.toInt; row.zipWithIndex.map { case (v,j) => (k,(j,v)) }
        }.partitionBy(part)

        val tKer = System.nanoTime()
        val coo  = aByK.join(bByK).map{ case (_ , ((i,a),(j,b))) => ((i,j), a*b) }.reduceByKey(part, _ + _).persist()
        coo.count()
        lastRDDKernelMs = (System.nanoTime() - tKer) / 1e6

        coo.unpersist(false); aC.unpersist(false); bC.unpersist(false)
      }
    }
    lastRDDKernelMs
  }

  /** 原有 Run：端到端（加载→转换→计算→稠密化→拼接），新增“左矩阵必须稀疏”的入口检查 */
  def Run(address1: String, address2: String): String = {
    import RunnerFuncs._

    val SMraw   = sc.textFile(address1)
    val fileraw = sc.textFile(address2)

    val SM   = SMraw.filter(RunnerFuncs.isNumericLine _)
    val file = fileraw.filter(RunnerFuncs.isNumericLine _)

    // <<< 新增：左矩阵必须稀疏（非零率 < 0.5）
    if (!RunnerFuncs.isSparse(SM)) {
      throw new IllegalArgumentException("左侧矩阵不是稀疏矩阵（非零率≥50%）。请提供稀疏输入。")
    }

    val numRowsRight = file.count().toInt
    if (numRowsRight == 0) return ""

    val numColsRight = file.first().split(",").length

    if (numColsRight == 1) {
      // ------- A · x -------
      if (isSparse(file)) {
        val (csrByRow, aShape)                = Con.SMToJoinableByRow(SM)
        val (svIdx, svVal, vlen)              = Con.ReadSV(file)
        require(aShape._2 == vlen, s"A.cols=${aShape._2}, |x|=$vlen")
        val y: RDD[Double] = Cal.SpM_SpSV(csrByRow, svIdx, svVal, aShape, vlen)
        println("SpM_SpSV")
        RunnerFuncs.vectorToCsvLine(y)
      } else {
        val (csrByRow, aShape) = Con.SMToJoinableByRow(SM)
        val (dv, vlen)         = Con.ReadDV(file)
        println("b")
        require(aShape._2 == vlen, s"A.cols=${aShape._2}, |x|=$vlen")
        println("a")
        val y: RDD[Double] = Cal.SpM_DV(csrByRow, dv, aShape)
        println("SpM_DV")
        RunnerFuncs.vectorToCsvLine(y)
      }
    } else {
      // ------- A · B -------
      if (isSparse(file)) {
        val (a_byRow, aShape) = Con.SMToJoinableByRow(SM)
        val (b_byCol, bShape) = Con.SMToJoinableByCol(file)
        require(aShape._2 == bShape._1,
          s"A.cols=${aShape._2} must equal B.rows=${bShape._1}")
        val c: RDD[(Int, Int, Double)] = Cal.SpM_SpM(a_byRow, b_byCol, aShape, bShape)
        println("SpM_SpM")
        RunnerFuncs.cooToDenseString(c, (aShape._1, bShape._2))
      } else {
        val (a_byRow, aShape) = Con.SMToJoinableByRow(SM)
        val (bDM, bShape)     = Con.ReadDM(file)
        require(aShape._2 == bShape._1,
          s"A.cols=${aShape._2} must equal B.rows=${bShape._1}")
        val c: RDD[(Int, Int, Double)] = Cal.SpM_SpDM(a_byRow, bDM, aShape, bShape)
        println("SpM_SpDM")
        RunnerFuncs.cooToDenseString(c, (aShape._1, bShape._2))
      }
    }
  }
}



//import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
//import org.apache.spark.rdd.RDD
//import scala.util.Try
//import scala.collection.mutable
//
//// ----------- 可序列化的函数集合：闭包只捕获这个对象，不会捕获 Runner.this -----------
//object RunnerFuncs extends Serializable {
//
//  /** 只保留纯数字的 CSV 行（过滤掉 GUI 提示/注释等） */
//  def isNumericLine(line: String): Boolean = {
//    val it = line.split(",").iterator
//    var any = false; var ok = true
//    while (ok && it.hasNext) {
//      val s = it.next().trim
//      if (s.nonEmpty) {
//        if (Try(s.toDouble).isFailure) ok = false else any = true
//      }
//    }
//    any && ok
//  }
//
//  /** 稀疏性判定（忽略非数字 token） */
//  def isSparse(matrix: RDD[String]): Boolean = {
//    val (nnz, total) = matrix.mapPartitions { it =>
//      var nonZero = 0L; var count = 0L
//      it.foreach { line =>
//        val arr = line.split(","); var i = 0
//        while (i < arr.length) {
//          val s = arr(i).trim
//          if (s.nonEmpty) {
//            Try(s.toDouble).toOption.foreach { d =>
//              count += 1; if (d != 0.0) nonZero += 1
//            }
//          }
//          i += 1
//        }
//      }
//      Iterator.single((nonZero, count))
//    }.reduce { case ((a1, a2), (b1, b2)) => (a1 + b1, a2 + b2) }
//
//    val rate = if (total == 0) 0.0 else nnz.toDouble / total.toDouble
//    println(f"[isS] nonzero rate = $rate%.4f , total=$total")
//    rate < 0.5
//  }
//
//  // 向量 -> 单行 CSV（保证全局顺序）
//  def vectorToCsvLine(vec: RDD[Double]): String = {
//    val ordered: RDD[String] =
//      vec.zipWithIndex()                         // (v, i)
//        .map{ case (v,i) => (i, v) }
//        .sortByKey()                            // 全局有序
//        .map{ _._2.toString }
//
//    val onePart: RDD[String] =
//      ordered.coalesce(1, shuffle = true)       // 收到一个分区，保持顺序
//        .mapPartitions(it => Iterator.single(it.mkString(",")))
//
//    onePart.first()
//  }
//
//  // COO -> 稠密矩阵多行字符串（保证行的全局顺序）
//  def cooToDenseString(coo: RDD[(Int, Int, Double)],
//                       shape: (Int, Int))
//                      (implicit sc: SparkContext): String = {
//    val (numRows, numCols) = shape
//    val P    = sc.defaultParallelism
//    val part = new HashPartitioner(P)
//
//    val rows: RDD[(Int, (Int, Double))] =
//      coo.map { case (i, j, v) => (i, (j, v)) }
//
//    val rowMap: RDD[(Int, mutable.Map[Int, Double])] =
//      rows.partitionBy(part).combineByKey[mutable.Map[Int, Double]](
//        (cv: (Int, Double)) => { val m = mutable.Map[Int, Double](); m.update(cv._1, cv._2); m },
//        (m: mutable.Map[Int, Double], cv: (Int, Double)) => { m.update(cv._1, cv._2); m },
//        (m1: mutable.Map[Int, Double], m2: mutable.Map[Int, Double]) => {
//          if (m2.size > m1.size) { m2 ++= m1; m2 } else { m1 ++= m2; m1 }
//        }
//      )
//
//    val allRows: RDD[(Int, Unit)] =
//      sc.parallelize(0 until numRows, P).map(i => (i, ()))
//
//    val denseLinesSorted: RDD[String] =
//      allRows.leftOuterJoin(rowMap)
//        .sortByKey(numPartitions = P)            // 行号有序
//        .map { case (_, (_, mOpt)) =>
//          val m = mOpt.getOrElse(mutable.Map.empty[Int, Double])
//          val sb = new StringBuilder
//          var j = 0
//          while (j < numCols) {
//            if (j > 0) sb.append(", ")
//            sb.append(m.getOrElse(j, 0.0))
//            j += 1
//          }
//          sb.toString
//        }
//
//    val onePart: RDD[String] =
//      denseLinesSorted.coalesce(1, shuffle = true)
//        .mapPartitions(it => Iterator.single(it.mkString("\n")))
//
//    onePart.first()
//  }
//}
//
//// ------------------------------ Runner 本体 ------------------------------
//class Runner {
//
//  // Windows 兜底（winutils）
//  private val isWin = System.getProperty("os.name").toLowerCase.contains("win")
//  if (isWin) {
//    val hh = sys.env.getOrElse("HADOOP_HOME", "C:\\hadoop")
//    System.setProperty("hadoop.home.dir", hh)
//  }
//
//  // Spark
//  private val conf: SparkConf = new SparkConf()
//    .setAppName("CW1")
//    .setMaster("local[*]")
//    .set("spark.ui.enabled", "false")
//    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//
//  implicit val sc: SparkContext = new SparkContext(conf)
//  sc.setLogLevel("WARN")
//
//  // 组件（放在 sc 之后）
//  val Cal = new Calculator
//  val Con = new Converter
//
//  // 主入口：返回字符串
//  def Run(address1: String, address2: String): String = {
//    import RunnerFuncs._
//
//    val SMraw   = sc.textFile(address1)
//    val fileraw = sc.textFile(address2)
//
//    // 过滤非数字行（不要用类方法，避免捕获 this）
//    val SM   = SMraw.filter(RunnerFuncs.isNumericLine _)
//    val file = fileraw.filter(RunnerFuncs.isNumericLine _)
//
//    val numRowsRight = file.count().toInt
//    if (numRowsRight == 0) return ""
//
//    val numColsRight = file.first().split(",").length
//
//    if (numRowsRight == 1) {
//      // ------- A · x -------
//      if (isSparse(file)) {
//        val (csrByRow, aShape)                = Con.SMToJoinableByRow(SM)
//        val (svIdx, svVal, vlen)              = Con.ReadSV(file)
//        require(aShape._2 == vlen, s"A.cols=${aShape._2}, |x|=$vlen")
//        val y: RDD[Double] = Cal.SpM_SpSV(csrByRow, svIdx, svVal, aShape, vlen)
//        println("SpM_SpSV")
//        vectorToCsvLine(y)
//      } else {
//        val (csrByRow, aShape) = Con.SMToJoinableByRow(SM)
//        val (dv, vlen)         = Con.ReadDV(file)
//        require(aShape._2 == vlen, s"A.cols=${aShape._2}, |x|=$vlen")
//        val y: RDD[Double] = Cal.SpM_DV(csrByRow, dv, aShape)
//        println("SpM_DV")
//        vectorToCsvLine(y)
//      }
//    } else {
//      // ------- A · B -------
//      if (isSparse(file)) {
//        val (a_byRow, aShape) = Con.SMToJoinableByRow(SM)
//        val (b_byCol, bShape) = Con.SMToJoinableByCol(file)
//        require(aShape._2 == bShape._1,
//          s"A.cols=${aShape._2} must equal B.rows=${bShape._1}")
//        val c: RDD[(Int, Int, Double)] = Cal.SpM_SpM(a_byRow, b_byCol, aShape, bShape)
//        println("SpM_SpM")
//        cooToDenseString(c, (aShape._1, bShape._2))
//      } else {
//        val (a_byRow, aShape) = Con.SMToJoinableByRow(SM)
//        val (bDM, bShape)     = Con.ReadDM(file)
//        require(aShape._2 == bShape._1,
//          s"A.cols=${aShape._2} must equal B.rows=${bShape._1}")
//        val c: RDD[(Int, Int, Double)] = Cal.SpM_SpDM(a_byRow, bDM, aShape, bShape)
//        println("SpM_SpDM")
//        cooToDenseString(c, (aShape._1, bShape._2))
//      }
//    }
//  }
//}
