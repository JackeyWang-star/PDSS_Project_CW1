package com.pdss.testdata

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import java.io._
import scala.util.Random

/**
 * 综合测试数据生成器 - 修正版
 * 生成各种规模和稀疏模式的矩阵，用于性能测试
 *
 * 修正点：支持行/列向量写出。默认仍为单行（行向量），
 *        在确需列向量的生成处显式传入 asColumn = true
 */
object TestDataGenerator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("PDSS Test Data Generator")
      .setMaster("local[*]")
      .set("spark.driver.memory", "2g")

    val sc = new SparkContext(conf)

    try {
      println("=" * 80)
      println("PDSS 测试数据生成器 (修正版，支持列向量写出)")
      println("=" * 80)

      // 创建输出目录
      val baseDir = new File("test_data")
      if (!baseDir.exists()) baseDir.mkdirs()

      // 生成各种测试数据
      generateSmallTests(baseDir)
      generateMediumTests(baseDir, sc)
      generateLargeTests(baseDir, sc)
      generateSpecialPatterns(baseDir, sc)
      generateBenchmarkSuite(baseDir, sc)

      println("\n✅ 所有测试数据已生成到: " + baseDir.getAbsolutePath)
      println("✅ 向量文件现在可按需要写为『行向量(单行CSV)』或『列向量(一列多行)』")

    } finally {
      sc.stop()
    }
  }

  /**
   * 1. 小型测试矩阵 - 用于验证正确性
   * 这里的三个小向量保留原先『单行』形式，便于肉眼核对
   */
  def generateSmallTests(baseDir: File): Unit = {
    println("\n生成小型测试矩阵...")

    val smallDir = new File(baseDir, "small")
    smallDir.mkdirs()

    // Test 1: 简单3x3稀疏矩阵
    writeMatrix(new File(smallDir, "matrix_3x3_sparse.csv"),
      Array(
        "4,0,9",
        "0,7,0",
        "0,0,5"
      ))

    // Test 2: 3x3稠密矩阵
    writeMatrix(new File(smallDir, "matrix_3x3_dense.csv"),
      Array(
        "1,2,3",
        "4,5,6",
        "7,8,9"
      ))

    // 小向量：保持单行（行向量）
    writeMatrix(new File(smallDir, "vector_3.csv"),
      Array("1", "2", "3"))

    writeMatrix(new File(smallDir, "vector_5.csv"),
      Array("1", "0", "2", "0", "3"))

    writeMatrix(new File(smallDir, "vector_4.csv"),
      Array("1", "2", "3", "4"))

    // Test 4: 单位矩阵
    writeMatrix(new File(smallDir, "identity_5x5.csv"),
      Array(
        "1,0,0,0,0",
        "0,1,0,0,0",
        "0,0,1,0,0",
        "0,0,0,1,0",
        "0,0,0,0,1"
      ))

    // Test 5: 全零矩阵
    writeMatrix(new File(smallDir, "zero_4x4.csv"),
      Array(
        "0,0,0,0",
        "0,0,0,0",
        "0,0,0,0",
        "0,0,0,0"
      ))

    // Test 6: 对角矩阵
    writeMatrix(new File(smallDir, "diagonal_4x4.csv"),
      Array(
        "5,0,0,0",
        "0,3,0,0",
        "0,0,8,0",
        "0,0,0,2"
      ))

    // Test 7: 上三角矩阵
    writeMatrix(new File(smallDir, "upper_triangular_4x4.csv"),
      Array(
        "1,2,3,4",
        "0,5,6,7",
        "0,0,8,9",
        "0,0,0,10"
      ))

    // Test 8: 矩形矩阵 3x5
    writeMatrix(new File(smallDir, "rect_3x5.csv"),
      Array(
        "1,0,2,0,3",
        "0,4,0,5,0",
        "6,0,7,0,8"
      ))

    // Test 9: 矩形矩阵 5x3
    writeMatrix(new File(smallDir, "rect_5x3.csv"),
      Array(
        "1,2,3",
        "0,0,0",
        "4,5,6",
        "0,0,0",
        "7,8,9"
      ))

    println(s"  ✓ 生成了9个小型测试矩阵")
    println(s"  ✓ 生成了3个测试向量（保留单行格式）")
  }

  /**
   * 2. 中型测试矩阵 - 用于功能测试
   * 这里的配套向量改为列向量（asColumn = true），方便直接 A * x
   */
  def generateMediumTests(baseDir: File, sc: SparkContext): Unit = {
    println("\n生成中型测试矩阵...")

    val mediumDir = new File(baseDir, "medium")
    mediumDir.mkdirs()

    // 100x100矩阵，不同稀疏度
    val sizes = List(
      (100, 100, 0.01, "100x100_1pct"),
      (100, 100, 0.05, "100x100_5pct"),
      (100, 100, 0.10, "100x100_10pct"),
      (100, 100, 0.30, "100x100_30pct")
    )

    sizes.foreach { case (rows, cols, sparsity, name) =>
      generateSparseMatrix(sc, rows, cols, sparsity,
        new File(mediumDir, s"matrix_$name.csv"))
      println(s"  ✓ $name (${rows}x${cols}, ${(sparsity*100).toInt}% 非零)")
    }

    // 向量：以列向量写出（与上面的 100x100、及后面的 50x100 / 100x50 场景匹配）
    generateDenseVector(100, new File(mediumDir, "vector_100.csv"), asColumn = true)
    generateSparseVector(100, 0.1, new File(mediumDir, "sparse_vector_100.csv"), asColumn = true)
    generateDenseVector(50, new File(mediumDir, "vector_50.csv"), asColumn = true)

    // 矩形矩阵测试
    generateSparseMatrix(sc, 50, 100, 0.05,
      new File(mediumDir, "matrix_50x100_5pct.csv"))
    generateSparseMatrix(sc, 100, 50, 0.05,
      new File(mediumDir, "matrix_100x50_5pct.csv"))

    println(s"  ✓ 生成了${sizes.size + 2}个中型测试矩阵")
    println(s"  ✓ 生成了3个中型向量（以列向量形式写出）")
  }

  /**
   * 3. 大型测试矩阵 - 用于性能测试
   * 这些配套向量一般用于 A * x，改为列向量
   */
  def generateLargeTests(baseDir: File, sc: SparkContext): Unit = {
    println("\n生成大型测试矩阵...")

    val largeDir = new File(baseDir, "large")
    largeDir.mkdirs()

    val largeSizes = List(
      (1000, 1000, 0.01, "1k_1pct"),
      (1000, 1000, 0.05, "1k_5pct"),
      (5000, 5000, 0.001, "5k_0.1pct"),
      (5000, 5000, 0.01, "5k_1pct"),
      (10000, 10000, 0.001, "10k_0.1pct"),
      (10000, 10000, 0.005, "10k_0.5pct")
    )

    largeSizes.foreach { case (rows, cols, sparsity, name) =>
      println(s"  生成 $name...")
      generateSparseMatrixEfficient(sc, rows, cols, sparsity,
        new File(largeDir, s"matrix_$name.csv"))
    }

    // 对应的列向量
    if (!new File(largeDir, "vector_1000.csv").exists()) {
      println("  生成 vector_1000.csv (列向量)...")
      generateDenseVector(1000, new File(largeDir, "vector_1000.csv"), asColumn = true)
    }
    if (!new File(largeDir, "vector_5000.csv").exists()) {
      println("  生成 vector_5000.csv (列向量)...")
      generateDenseVector(5000, new File(largeDir, "vector_5000.csv"), asColumn = true)
    }
    if (!new File(largeDir, "vector_10000.csv").exists()) {
      println("  生成 vector_10000.csv (列向量)...")
      generateDenseVector(10000, new File(largeDir, "vector_10000.csv"), asColumn = true)
    }

    println(s"  ✓ 生成了${largeSizes.size}个大型测试矩阵")
    println(s"  ✓ 生成了3个大型向量（以列向量形式写出）")
  }

  /**
   * 4. 特殊模式矩阵 - 测试不同稀疏结构
   */
  def generateSpecialPatterns(baseDir: File, sc: SparkContext): Unit = {
    println("\n生成特殊模式矩阵...")

    val patternDir = new File(baseDir, "patterns")
    patternDir.mkdirs()

    // 带状矩阵（三对角）
    generateBandedMatrix(1000, 1000, 1,
      new File(patternDir, "tridiagonal_1000.csv"))

    // 宽带矩阵
    generateBandedMatrix(1000, 1000, 10,
      new File(patternDir, "banded_1000_width10.csv"))

    // 块对角矩阵
    generateBlockDiagonalMatrix(sc, 1000, 1000, 10, 0.5,
      new File(patternDir, "block_diagonal_1000.csv"))

    // 幂律分布（模拟真实图数据）
    generatePowerLawMatrix(sc, 1000, 1000, 10000,
      new File(patternDir, "power_law_1000.csv"))

    // 随机行/列模式（某些行/列特别稠密）
    generateRowHotspotMatrix(sc, 1000, 1000, 0.01, 10,
      new File(patternDir, "row_hotspot_1000.csv"))

    println("  ✓ 生成了5种特殊模式矩阵")
  }

  /**
   * 5. 基准测试套件 - 标准化性能测试
   * 本套件的向量全部按『列向量』写出，匹配 A * x 的常见用法
   */
  def generateBenchmarkSuite(baseDir: File, sc: SparkContext): Unit = {
    println("\n生成基准测试套件...")

    val benchDir = new File(baseDir, "benchmark")
    benchDir.mkdirs()

    // 生成README
    val readme = new File(benchDir, "README.txt")
    val pw = new PrintWriter(readme)
    pw.println("PDSS 基准测试套件")
    pw.println("=" * 50)
    pw.println()
    pw.println("说明：向量文件可为『行向量(单行CSV)』或『列向量(一列多行)』。")
    pw.println("本基准中的向量均为列向量，便于直接进行 A * x。")
    pw.println()
    pw.println("测试集说明：")
    pw.println()

    // 可扩展性测试集 - 固定稀疏度，增加大小
    val scalabilitySizes = List(500, 1000, 2000, 4000)
    pw.println("1. 可扩展性测试集（Scalability Test）")
    pw.println("   固定稀疏度 1%，矩阵大小递增")

    scalabilitySizes.foreach { size =>
      val fileName = s"scale_${size}x${size}.csv"
      println(s"  生成 $fileName...")
      generateSparseMatrixEfficient(sc, size, size, 0.01,
        new File(benchDir, fileName))

      println(s"  生成 vector_$size.csv (列向量)...")
      generateDenseVector(size, new File(benchDir, s"vector_$size.csv"), asColumn = true)

      pw.println(s"   - $fileName: ${size}x${size}, 1% sparse")
      pw.println(s"   - vector_$size.csv: ${size}x1 列向量")
    }

    // 固定2000大小的向量（列向量）
    if (!new File(benchDir, "vector_2000.csv").exists()) {
      println("  生成 vector_2000.csv (列向量)...")
      generateDenseVector(2000, new File(benchDir, "vector_2000.csv"), asColumn = true)
    }

    // 稀疏度测试集 - 固定大小，变化稀疏度
    val sparsityLevels = List(0.001, 0.005, 0.01, 0.05, 0.1, 0.2)
    pw.println()
    pw.println("2. 稀疏度测试集（Sparsity Test）")
    pw.println("   固定大小 2000x2000，稀疏度递增")

    sparsityLevels.foreach { sparsity =>
      val pctStr = f"${sparsity*100}%.1f".replace(".", "_")
      val fileName = s"sparsity_2000x2000_${pctStr}pct.csv"
      println(s"  生成 $fileName...")
      generateSparseMatrixEfficient(sc, 2000, 2000, sparsity,
        new File(benchDir, fileName))
      pw.println(s"   - $fileName: 2000x2000, ${sparsity*100}% sparse")
    }

    // 形状测试集 - 不同长宽比
    pw.println()
    pw.println("3. 形状测试集（Shape Test）")
    val shapes = List(
      (1000, 100, "tall"),
      (100, 1000, "wide"),
      (1000, 1000, "square")
    )

    shapes.foreach { case (rows, cols, shape) =>
      val fileName = s"shape_${shape}_${rows}x${cols}.csv"
      println(s"  生成 $fileName...")
      generateSparseMatrixEfficient(sc, rows, cols, 0.01,
        new File(benchDir, fileName))

      // 为矩形矩阵生成合适的列向量
      if (shape == "tall") {
        generateDenseVector(100, new File(benchDir, "vector_100_for_tall.csv"), asColumn = true)
      } else if (shape == "wide") {
        generateDenseVector(1000, new File(benchDir, "vector_1000_for_wide.csv"), asColumn = true)
      }

      pw.println(s"   - $fileName: ${rows}x${cols}, 1% sparse")
    }

    pw.close()
    println(s"  ✓ 生成了完整的基准测试套件")
  }

  // ============= 辅助方法 =============

  /**
   * 生成稀疏矩阵（小型，直接生成）
   */
  def generateSparseMatrix(sc: SparkContext, rows: Int, cols: Int,
                           sparsity: Double, file: File): Unit = {
    val rand = new Random(42)
    val matrix = Array.ofDim[Double](rows, cols)
    val nnz = (rows * cols * sparsity).toInt

    // 随机填充非零元素
    for (_ <- 0 until nnz) {
      val i = rand.nextInt(rows)
      val j = rand.nextInt(cols)
      if (matrix(i)(j) == 0.0) {  // 避免重复位置
        matrix(i)(j) = rand.nextGaussian() * 10
      }
    }

    // 写入CSV
    val lines = matrix.map(row => row.map(v => f"$v%.2f").mkString(","))
    writeMatrix(file, lines)
  }

  /**
   * 生成稀疏矩阵（大型，使用RDD避免内存溢出）
   */
  def generateSparseMatrixEfficient(sc: SparkContext, rows: Int, cols: Int,
                                    sparsity: Double, file: File): Unit = {
    // 使用RDD生成，避免内存问题
    val matrixRDD = sc.parallelize(0 until rows).map { i =>
      val rand = new Random(42 + i)
      val rowNnz = Math.max(1, (cols * sparsity).toInt)
      val row = Array.fill(cols)(0.0)

      // 随机选择列位置
      val positions = scala.util.Random.shuffle((0 until cols).toList).take(rowNnz)
      positions.foreach { j =>
        row(j) = (rand.nextGaussian() * 10)
      }

      row.map(v => f"$v%.2f").mkString(",")
    }

    // 收集并写入（逐行写入，避免一次性加载）
    val pw = new PrintWriter(file)
    try {
      matrixRDD.collect().foreach(pw.println)
    } finally {
      pw.close()
    }
  }

  /**
   * 生成带状矩阵
   */
  def generateBandedMatrix(rows: Int, cols: Int, bandwidth: Int, file: File): Unit = {
    val pw = new PrintWriter(file)
    try {
      for (i <- 0 until rows) {
        val row = Array.fill(cols)(0.0)
        for (j <- Math.max(0, i - bandwidth) to Math.min(cols - 1, i + bandwidth)) {
          if (j < cols) {
            row(j) = Random.nextGaussian() * 10
          }
        }
        pw.println(row.map(v => f"$v%.2f").mkString(","))
      }
    } finally {
      pw.close()
    }
  }

  /**
   * 生成块对角矩阵
   */
  def generateBlockDiagonalMatrix(sc: SparkContext, rows: Int, cols: Int,
                                  blockSize: Int, blockDensity: Double, file: File): Unit = {
    val numBlocks = Math.min(rows, cols) / blockSize

    val matrixRDD = sc.parallelize(0 until rows).map { i =>
      val row = Array.fill(cols)(0.0)
      val blockId = i / blockSize

      if (blockId < numBlocks) {
        val blockStartCol = blockId * blockSize
        val blockEndCol = Math.min(blockStartCol + blockSize, cols)

        for (j <- blockStartCol until blockEndCol) {
          if (Random.nextDouble() < blockDensity) {
            row(j) = Random.nextGaussian() * 10
          }
        }
      }

      row.map(v => f"$v%.2f").mkString(",")
    }

    val pw = new PrintWriter(file)
    try {
      matrixRDD.collect().foreach(pw.println)
    } finally {
      pw.close()
    }
  }

  /**
   * 生成幂律分布矩阵（模拟真实网络）
   */
  def generatePowerLawMatrix(sc: SparkContext, rows: Int, cols: Int,
                             nnz: Long, file: File): Unit = {
    // 生成幂律分布的度数
    val rowDegrees = Array.fill(rows)(0)
    val colDegrees = Array.fill(cols)(0)

    // 使用优先连接生成幂律分布
    val rand = new Random(42)
    for (_ <- 0L until nnz) {
      val i = if (rowDegrees.sum == 0) {
        rand.nextInt(rows)
      } else {
        val weights = rowDegrees.map(d => Math.max(1, d))
        selectWeighted(weights, rand)
      }

      val j = rand.nextInt(cols)
      rowDegrees(i) += 1
      colDegrees(j) += 1
    }

    // 根据度数分布生成矩阵
    val matrixRDD = sc.parallelize(0 until rows).map { i =>
      val row = Array.fill(cols)(0.0)
      val degree = rowDegrees(i)

      if (degree > 0) {
        val positions = Random.shuffle((0 until cols).toList).take(Math.min(degree, cols))
        positions.foreach { j =>
          row(j) = Random.nextGaussian() * 10
        }
      }

      row.map(v => f"$v%.2f").mkString(",")
    }

    val pw = new PrintWriter(file)
    try {
      matrixRDD.collect().foreach(pw.println)
    } finally {
      pw.close()
    }
  }

  /**
   * 生成行热点矩阵（某些行特别稠密）
   */
  def generateRowHotspotMatrix(sc: SparkContext, rows: Int, cols: Int,
                               baseSparsity: Double, hotspotRows: Int, file: File): Unit = {
    val hotspotIndices = Random.shuffle((0 until rows).toList).take(hotspotRows).toSet

    val matrixRDD = sc.parallelize(0 until rows).map { i =>
      val row = Array.fill(cols)(0.0)
      val sparsity = if (hotspotIndices.contains(i)) 0.5 else baseSparsity
      val nnz = (cols * sparsity).toInt

      val positions = Random.shuffle((0 until cols).toList).take(nnz)
      positions.foreach { j =>
        row(j) = Random.nextGaussian() * 10
      }

      row.map(v => f"$v%.2f").mkString(",")
    }

    val pw = new PrintWriter(file)
    try {
      matrixRDD.collect().foreach(pw.println)
    } finally {
      pw.close()
    }
  }

  /**
   * 生成稠密向量
   * 新增 asColumn 参数：true -> 列向量（一列多行）；false -> 行向量（单行CSV）
   */
  def generateDenseVector(size: Int, file: File, asColumn: Boolean = false): Unit = {
    val vector = if (size <= 10) {
      (1 to size).map(_.toDouble).toArray
    } else {
      Array.fill(size)(Random.nextGaussian() * 10)
    }
    writeVector(file, vector, asColumn)
  }

  /**
   * 生成稀疏向量
   * 新增 asColumn 参数：true -> 列向量；false -> 行向量
   */
  def generateSparseVector(size: Int, sparsity: Double, file: File, asColumn: Boolean = false): Unit = {
    val vector = Array.fill(size)(0.0)
    val nnz = Math.max(1, (size * sparsity).toInt)
    val positions = Random.shuffle((0 until size).toList).take(nnz)
    positions.foreach { i => vector(i) = Random.nextGaussian() * 10 }
    writeVector(file, vector, asColumn)
  }

  /**
   * 写向量到文件：asColumn 控制行/列格式
   */
  private def writeVector(file: File, values: Array[Double], asColumn: Boolean): Unit = {
    val pw = new PrintWriter(file)
    try {
      if (asColumn) {
        // 列向量：一列多行
        values.foreach(v => pw.println(f"$v%.2f"))
      } else {
        // 行向量：单行CSV
        pw.println(values.map(v => f"$v%.2f").mkString(","))
      }
    } finally {
      pw.close()
    }
  }

  /**
   * 根据权重随机选择
   */
  def selectWeighted(weights: Array[Int], rand: Random): Int = {
    val total = weights.sum
    if (total == 0) return rand.nextInt(weights.length)

    val r = rand.nextInt(total)
    var sum = 0
    for (i <- weights.indices) {
      sum += weights(i)
      if (sum > r) return i
    }
    weights.length - 1
  }

  /**
   * 写入矩阵到文件
   */
  def writeMatrix(file: File, lines: Array[String]): Unit = {
    val pw = new PrintWriter(file)
    try {
      lines.foreach(pw.println)
    } finally {
      pw.close()
    }
  }
}

/**
 * 验证生成的文件格式（支持行/列向量识别）
 */
object TestDataValidator {

  private def isNumeric(s: String): Boolean = {
    val t = s.trim
    if (t.isEmpty) false
    else {
      try { t.toDouble; true } catch { case _: Throwable => false }
    }
  }

  def validateVectorFormat(file: File): Unit = {
    val lines = scala.io.Source.fromFile(file).getLines().toList

    println(s"\n验证文件: ${file.getName}")
    println(s"  行数: ${lines.length}")

    if (lines.isEmpty) {
      println(s"  ❌ 空文件")
      return
    }

    if (lines.length == 1) {
      // 单行 => 行向量
      val elements = lines.head.split(",")
      val allNums = elements.forall(isNumeric)
      if (allNums) {
        println(s"  ✅ 行向量 (1 x ${elements.length})")
        if (elements.length <= 10) println(s"  内容: ${lines.head}")
        else println(s"  前10个元素: ${elements.take(10).mkString(",")}")
      } else {
        println(s"  ❌ 单行但存在非数值元素")
      }
    } else {
      // 多行，尝试判定是否列向量（每行一个数）
      val looksLikeCol = lines.forall { ln =>
        val parts = ln.split(",")
        parts.length == 1 && isNumeric(parts.headOption.getOrElse(""))
      }
      if (looksLikeCol) {
        println(s"  ✅ 列向量 (${lines.length} x 1)")
        if (lines.length <= 10) println(s"  前${lines.length}个元素: ${lines.mkString(",")}")
        else println(s"  前10个元素: ${lines.take(10).mkString(",")}")
      } else {
        println(s"  ❌ 既不是纯行向量，也不是纯列向量（可能是矩阵或格式异常）")
      }
    }
  }

  def main(args: Array[String]): Unit = {
    println("\n验证生成的向量文件格式...")
    println("=" * 60)

    // 小型向量（保持行向量）
    val smallVectors = List("vector_3.csv", "vector_5.csv", "vector_4.csv")
    smallVectors.foreach { name =>
      val file = new File(s"test_data/small/$name")
      if (file.exists()) validateVectorFormat(file)
    }

    // 中型向量（现在为列向量）
    val mediumVectors = List("vector_50.csv", "vector_100.csv", "sparse_vector_100.csv")
    mediumVectors.foreach { name =>
      val file = new File(s"test_data/medium/$name")
      if (file.exists()) validateVectorFormat(file)
    }

    println("\n提示：基准套件与大型向量也已切换为列向量（用于 A * x）。")
  }
}
