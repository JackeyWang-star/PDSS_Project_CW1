import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD

class Calculator(implicit sc: SparkContext) {

  // SpM_DV 的 "纯 RDD" 版本
  def SpM_DV(
              matrixByRow: RDD[(Int, (Int, Double))], // <-- 新的 RDD 输入
              vector: RDD[Double],
              shape: (Int, Int)
            ): RDD[Double] = {

    val numRows = shape._1
    val numCols = shape._2
    val vecLength = vector.count().toInt

    if (numCols != vecLength) {
      println(s"[Error] Matrix-Vector shape mismatch...")
      return sc.parallelize(Seq.empty[Double])
    }

    // 步骤 1: 稠密向量 -> (colId, vecVal)
    val vectorWithIndex = vector.zipWithIndex().map { case (v, i) => (i.toInt, v) }

    // 步骤 2: 矩阵 (row, (col, matVal)) -> (col, (row, matVal))
    val matrixByCol = matrixByRow.map {
      case (rowId, (colId, v)) => (colId, (rowId, v))
    }

    // 步骤 3: HashPartitioner 优化 join
    //val numPartitions = 40
    // 原：val numPartitions = 40
    val numPartitions = sc.defaultParallelism         // 优化让分区数随环境自适应
    val partitioner = new HashPartitioner(numPartitions)

    val partitionedVector = vectorWithIndex.partitionBy(partitioner)
    val partitionedMatrix = matrixByCol.partitionBy(partitioner)

    // 步骤 4: join 并计算乘积
    val joined = partitionedMatrix.join(partitionedVector)
      .map { case (_, ((rowId, v), vecVal)) => (rowId, v * vecVal) } // (rowId, partialProduct)

    // 步骤 5: 按行求和
    //val result = joined.reduceByKey(_ + _)
    val result = joined.reduceByKey(partitioner, _ + _) // 继承分区器，避免二次重分发

    // 步骤 6: 补零
    val rowIndices = sc.parallelize(0 until numRows).map(i => (i, ()))
    val fullResultKV = rowIndices
      .leftOuterJoin(result)
      .map { case (i, (_, valueOpt)) => (i, valueOpt.getOrElse(0.0)) }
      .sortByKey()
      .map(_._2)

    fullResultKV
  }

  // SpM_SpSV 的 "纯 RDD" 版本
  def SpM_SpSV(
                matrixByRow: RDD[(Int, (Int, Double))], // <-- 新的 RDD 输入
                vecIndices: RDD[Int],
                vecValues: RDD[Double],
                shape: (Int, Int),
                vecLength: Int
              ): RDD[Double] = {

    val numRows = shape._1
    val numCols = shape._2

    if (numCols != vecLength) {
      println(s"[Error] Dimension mismatch...")
      return sc.parallelize(Seq.empty[Double])
    }

    // 步骤 1: 稀疏向量 -> (colId, vecVal)
    val sparseVec = vecIndices.zip(vecValues).map{ case (idx, v) => (idx, v) }

    // 步骤 2: 矩阵 (row, (col, matVal)) -> (col, (row, matVal))
    val matrixByCol = matrixByRow.map {
      case (rowId, (colId, v)) => (colId, (rowId, v))
    }

    // === 细微优化开始：稀疏向量与矩阵都按“列键”共分区，并在规约时继承同一分区器 ===
    val P    = sc.defaultParallelism               // 原来是固定40，这里自适应并行度
    val part = new HashPartitioner(P)

    val sparseVecPart   = sparseVec.partitionBy(part)
    val matrixByColPart = matrixByCol.partitionBy(part)

//    // 步骤 3: join 并乘积
//    val joined = matrixByCol
//      .join(sparseVec)
//      .map { case (colId, ((rowId, v), vecVal)) => (rowId, v * vecVal) }

    // 步骤 3: join 并乘积（在共分区下进行，减少一次隐性 shuffle）
    val joined: RDD[(Int, Double)] =
      matrixByColPart
        .join(sparseVecPart)                       // key: colId
        .map { case (_ /*colId*/, ((rowId, aij), xj)) => (rowId, aij * xj) }

//    // 步骤 4: 按行求和
//    val summed = joined.reduceByKey(_ + _)

    // 步骤 4: 按行求和（继承 partitioner，避免再次重分发）
    val summed: RDD[(Int, Double)] =
      joined.reduceByKey(part, _ + _)

//    // 步骤 5: 构造完整稠密结果
//    val allRows = sc.parallelize(0 until numRows).map(i => (i, ()))
//    val result = allRows.leftOuterJoin(summed)
//      // [ 修复 ] 在 map 中同时保留键 'i' 和计算出的值
//      .map { case (i, (_, optVal)) => (i, optVal.getOrElse(0.0)) }
//      // 现在类型是 RDD[(Int, Double)]
//      .sortByKey() // <-- 现在可以工作了
//      .map(_._2)     // <-- 现在也可以工作了
//
//    result
// 步骤 5: 构造完整稠密结果（如不要求顺序，可去掉 sortByKey() 以避免一次全局排序）
val allRows: RDD[(Int, Unit)] =
  sc.parallelize(0 until numRows, P).map(i => (i, ()))

    val result: RDD[Double] =
      allRows.leftOuterJoin(summed)
        .map { case (i, (_, optVal)) => (i, optVal.getOrElse(0.0)) }
        .sortByKey(numPartitions = P)
        .map(_._2)

    result
  }

  // SpM_SpM 的 "纯 RDD" 版本
  def SpM_SpM(
               A_matrixByRow: RDD[(Int, (Int, Double))],  // <-- 新的 A 输入
               B_matrixByCol: RDD[(Int, (Int, Double))],  // <-- 新的 B 输入
               A_shape: (Int, Int),
               B_shape: (Int, Int)
             ): RDD[(Int, Int, Double)] = {

    val A_numRows = A_shape._1
    val A_numCols = A_shape._2
    val B_numRows = B_shape._1
    val B_numCols = B_shape._2

    if (A_numCols != B_numRows) {
      println(s"[Error] Dimension mismatch...")
      return sc.parallelize(Seq.empty[(Int, Int, Double)])
    }

    // 步骤 1: A -> (k, (i, A_ik))
    val aByK = A_matrixByRow.map { case (i, (k, v)) => (k, (i, v)) }

    // 步骤 2: B -> (k, (j, B_kj))
    val bByK = B_matrixByCol.map { case (j, (k, v)) => (k, (j, v)) }

    // 步骤 3: 优化并执行乘法
//    val numPartitions = 40
    val numPartitions = sc.defaultParallelism
    val partitioner = new HashPartitioner(numPartitions)

    val aByK_part = aByK.partitionBy(partitioner)
    val bByK_part = bByK.partitionBy(partitioner)

    aByK_part.join(bByK_part)
      .map { case (k, ((i, aVal), (j, bVal))) => ((i, j), aVal * bVal) }
//      .reduceByKey(_ + _)
      .reduceByKey(partitioner, _ + _)
      .map { case ((i, j), sum) => (i, j, sum) }
  }

  // SpM_SpDM 的 "纯 RDD" 版本
  def SpM_SpDM (
                 A_matrixByRow: RDD[(Int, (Int, Double))], // <-- 新的 A 输入
                 B_matrix: RDD[Array[Double]],
                 A_shape: (Int, Int),
                 B_shape: (Int, Int)
               ): RDD[(Int, Int, Double)] = {

    if (A_shape._2 != B_shape._1) {
      println(s"[Error] Dimension mismatch...")
      return sc.parallelize(Seq.empty[(Int, Int, Double)])
    }

    // 步骤 1: A -> (k, (i, A_ik))
    val aByK = A_matrixByRow.map { case (i, (k, v)) => (k, (i, v)) }

    // 步骤 2: B (Dense) -> RDD[(k, (j, B_kj))]
    val bByK = B_matrix.zipWithIndex().flatMap { // (RowArray, k)
      case (row, k_long) =>
        val k = k_long.toInt
        row.zipWithIndex.map { // (B_kj, j)
          case (v, j) =>
            (k, (j, v)) // (k, (j, B_kj))
        }
    }

    // 步骤 3: 优化并执行乘法
//    val numPartitions = 40
    val numPartitions = sc.defaultParallelism
    val partitioner = new HashPartitioner(numPartitions)

    val aByK_part = aByK.partitionBy(partitioner)
    val bByK_part = bByK.partitionBy(partitioner)

    aByK_part.join(bByK_part)
      .map { case (k, ((i, aVal), (j, bVal))) => ((i, j), aVal * bVal) }
//      .reduceByKey(_ + _)
      .reduceByKey(partitioner, _ + _)
      .map { case ((i, j), sum) => (i, j, sum) }
  }
}