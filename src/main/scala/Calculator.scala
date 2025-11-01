import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD

class Calculator {
  def csrMultiply(
                   rowOffset: RDD[Int],
                   colIndices: RDD[Int],
                   values: RDD[Double],
                   vector: RDD[Double],
                   shape: (Int, Int)
                 )(implicit sc: SparkContext): RDD[Double] = {

    val numRows = shape._1
    val numCols = shape._2
    val vecLength = vector.count().toInt

    // Step 0: 维度检查
    if (numCols != vecLength) {
      println(s"[Error] Matrix-Vector shape mismatch: Matrix is ${numRows}×${numCols}, " +
        s"but vector length is $vecLength. Multiplication not possible.")
      // 返回一个空 RDD，以防止程序崩溃
      return sc.parallelize(Seq.empty[Double])
    }

    // Step 1: 稠密向量转换为 (index, value)
    val vectorWithIndex = vector.zipWithIndex().map { case (v, i) => (i.toInt, v) }

    // Step 2: 给 colIndices、values 添加索引并合并
    val indexedCols = colIndices.zipWithIndex().map(_.swap)  // (idx, colId)
    val indexedVals = values.zipWithIndex().map(_.swap)      // (idx, value)
    val merged = indexedCols.join(indexedVals).map {
      case (idx, (colId, v)) => (idx.toInt, (colId, v)) //相同col和value的index的值合并
    }

    // Step 3：构造 (rowId, start, end) 行区间信息
    // 例如 rowOffset = [0, 2, 5, 7]  ->  (0, (0,2)), (1, (2,5)), (2, (5,7))
    val shifted = rowOffset.zipWithIndex().map { case (offset, i) => (i.toInt, offset) }
    val nextShifted = shifted.map { case (rowId, start) => (rowId - 1, start) }
    val rowRanges = shifted.join(nextShifted)
      .filter { case (_, (start, end)) => start < end } // 去除负数行
      .map { case (rowId, (start, end)) => (rowId, (end, start)) } // end-start 反转方便后面用

    // Step 4：根据区间范围展开 idx -> rowId（完全分布式）
    val idxRowMap = rowRanges.flatMap {
      case (rowId, (end, start)) =>
        Iterator.range(start, end).map(idx => (idx, rowId))
    }

    // Step 5：合并矩阵索引与行号
    val mergedWithRow = merged.join(idxRowMap).map {
      case (idx, ((colId, v), rowId)) => (rowId, (colId, v))
    }

    // Step 6：HashPartitioner 优化 join
    val numPartitions = 40
    val partitioner = new HashPartitioner(numPartitions)

    val partitionedVector = vectorWithIndex.partitionBy(partitioner)
    val partitionedMatrix = mergedWithRow
      .map { case (rowId, (colId, v)) => (colId, (rowId, v)) }
      .partitionBy(partitioner)

    // Step 7：join 向量并计算乘积
    val joined = partitionedMatrix.join(partitionedVector)
      .map { case (_, ((rowId, v), vecVal)) => (rowId, v * vecVal) }

    // Step 8：按行求和
    val result = joined
      .reduceByKey(_ + _)
      .sortByKey()

    // Step 9：若某些行全为零，补零
    val rowIndices = sc.parallelize(0 until numRows).map(i => (i, ()))
    val fullResultKV = rowIndices
      .leftOuterJoin(result)
      .map { case (i, (_, valueOpt)) => (i, valueOpt.getOrElse(0.0)) }
      .sortByKey()
      .map(_._2)

    fullResultKV
  }
}
