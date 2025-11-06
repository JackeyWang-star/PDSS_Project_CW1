import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD

class Calculator {
  def SpM_DV(
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

  def SpM_SpSV(
                rowOffset: RDD[Int],
                colIndices: RDD[Int],
                values: RDD[Double],
                vecIndices: RDD[Int],
                vecValues: RDD[Double],
                shape: (Int, Int),
                vecLength: Int
              )(implicit sc: SparkContext): RDD[Double] = {

    val numRows = shape._1
    val numCols = shape._2

    if (numCols != vecLength) {
      println(s"[Error] Dimension mismatch: Matrix ${numRows}×${numCols}, Vector length $vecLength")
      return sc.parallelize(Seq.empty[Double])
    }

    // Step 1: (idx, (colId, value))
    val merged = colIndices.zip(values).zipWithIndex().map {
      case ((c, v), idx) => (idx.toInt, (c, v))
    }

    // Step 2: 构造 row 范围 (rowId, (start, end))
    val rowOffsetsWithIndex = rowOffset.zipWithIndex().map { case (off, i) => (i.toInt, off) }
    val nextOffsets = rowOffsetsWithIndex.map { case (i, off) => (i - 1, off) }.filter(_._1 >= 0)
    val rowRanges = rowOffsetsWithIndex.join(nextOffsets).map {
      case (rowId, (start, end)) => (rowId, (start, end))
    }

    // Step 3: 生成 (idx, rowId) 的映射
    val posToRow = rowRanges.flatMap {
      case (rowId, (start, end)) => (start until end).map(pos => (pos, rowId))
    }

    // Step 4: 合并 (idx, (colId, value)) → (rowId, (colId, value))
    val mergedWithRow = merged.join(posToRow).map {
      case (idx, ((colId, v), rowId)) => (rowId, (colId, v))
    }

    // Step 5: 稀疏向量 (colId, vecVal)
    val sparseVec = vecIndices.zip(vecValues) // (colId, vecVal)

    // Step 6: join 并乘积
    val joined = mergedWithRow
      .map { case (rowId, (colId, v)) => (colId, (rowId, v)) }
      .join(sparseVec)
      .map { case (_, ((rowId, v), vecVal)) => (rowId, v * vecVal) }

    // Step 7: 按行求和
    val summed = joined.reduceByKey(_ + _)

    // Step 8: 构造完整稠密结果
    val allRows = sc.parallelize(0 until numRows).map(i => (i, ()))
    val result = allRows.leftOuterJoin(summed)
      .map { case (i, (_, optVal)) => optVal.getOrElse(0.0) }

    result
  }

  def SpM_SpM(
               A_rowOffset: RDD[Int],
               A_colIndices: RDD[Int],
               A_values: RDD[Double],
               B_colOffset: RDD[Int],
               B_rowIndices: RDD[Int],
               B_values: RDD[Double],
               A_shape: (Int, Int),
               B_shape: (Int, Int)
             )(implicit sc: SparkContext): RDD[(Int, Int, Double)] = {

    val A_numRows = A_shape._1
    val A_numCols = A_shape._2
    val B_numRows = B_shape._1
    val B_numCols = B_shape._2

    // 维度检查
    if (A_numCols != B_numRows) {
      println(s"[Error] Dimension mismatch: Matrix A ${A_numRows}×${A_numCols}, Matrix B ${B_numRows}×${B_numCols}")
      return sc.parallelize(Seq.empty[(Int, Int, Double)])
    }

    // ---------------------------
    // 处理A(CSR格式)：构建(row索引到行的映射
    // ---------------------------
    // 转换A_rowOffset为(rowId, (start, end))
    val aOffsetsWithIndex = A_rowOffset.zipWithIndex().map { case (off, idx) => (idx.toInt, off) }
    val aNextOffsets = aOffsetsWithIndex.map { case (idx, off) => (idx - 1, off) }.filter(_._1 >= 0)

    // 得到每行的元素范围 (rowId -> (startPos, endPos))
    val aRowRanges = aOffsetsWithIndex
      .join(aNextOffsets)
      .map { case (rowId, (start, end)) => (rowId, (start, end)) }


    // 构建位置映射到行号 (pos -> rowId)
    val aPosToRow = aRowRanges.flatMap { case (rowId, (start, end)) =>
      (start until end).map(pos => (pos, rowId))
    }
    // 将A转换为(i, (k, A_ik)) 其中i是行号，k是列号
    val aIndexedCols = A_colIndices.zipWithIndex().map { case (col, idx) => (idx.toInt, col) }
    val aIndexedVals = A_values.zipWithIndex().map { case (v, idx) => (idx.toInt, v) }
    val aEntries = aIndexedCols.join(aIndexedVals)  // (pos, (k, A_ik))
    val aByRow = aPosToRow.join(aEntries)          // (pos, (i, (k, A_ik)))
      .map { case (_, (i, (k, v))) => (i, (k, v)) }  // (i, (k, A_ik))

    //---------------------------
    //处理B(CSC格式)：构建位置索引到列的映射
    //---------------------------
    //转换B_colOffset为(colId, (start, end))
    val bOffsetsWithIndex = B_colOffset.zipWithIndex().map { case (off, idx) => (idx.toInt, off) }
    val bNextOffsets = bOffsetsWithIndex.map { case (idx, off) => (idx - 1, off) }.filter(_._1 >= 0)

    // 得到每列的元素范围 (colId -> (startPos, endPos))
    val bColRanges = bOffsetsWithIndex
      .join(bNextOffsets)
      .map { case (colId, (start, end)) => (colId, (start, end)) }


    // 位置映射到列号 (pos -> colId)
    val bPosToCol = bColRanges.flatMap { case (colId, (start, end)) =>
      (start until end).map(pos => (pos, colId))
    }

    // 将B转换为(j, (k, B_kj)) 其中j是列号，k是行号
    val bIndexedRows = B_rowIndices.zipWithIndex().map { case (row, idx) => (idx.toInt, row) }
    val bIndexedVals = B_values.zipWithIndex().map { case (v, idx) => (idx.toInt, v) }
    val bEntries = bIndexedRows.join(bIndexedVals)  // (pos, (k, B_kj))
    val bByCol = bPosToCol.join(bEntries)          // (pos, (j, (k, B_kj)))
      .map { case (_, (j, (k, v))) => (j, (k, v)) }  // (j, (k, B_kj))

    // ---------------------------
    // 矩阵乘法核心：通过k连接并计算
    // ---------------------------
    // 转换为以k为键：(k, (i, A_ik)) 和 (k, (j, B_kj))
    val aByK = aByRow.map { case (i, (k, v)) => (k, (i, v)) }//i是行号，k是列号
    val bByK = bByCol.map { case (j, (k, v)) => (k, (j, v)) }//j是列号，k是行号


    // 按k连接并计算乘积
    aByK.join(bByK)
      .map { case (k, ((i, aVal), (j, bVal))) => ((i, j), aVal * bVal) }
      .reduceByKey(_ + _)  // 累加相同(i,j)的乘积
      .map { case ((i, j), sum) => (i, j, sum) }  // 转换为三元组格式
  }

  def SpM_SpDM (
                 A_rowOffset: RDD[Int],
                 A_colIndices: RDD[Int],
                 A_values: RDD[Double],
                 B_matrix: RDD[Array[Double]],
                 A_shape: (Int, Int),
                 B_shape: (Int, Int)
               )(implicit sc: SparkContext): RDD[(Int, Int, Double)] ={
    if (A_shape._2 != B_shape._1) {
      println(s"[Error] Dimension mismatch: Matrix A ${A_shape._1}×${A_shape._2}, Matrix B ${B_shape._1}×${B_shape._2}")
      return sc.parallelize(Seq.empty[(Int, Int, Double)])
    }

    //A_metrix processing
    val aOffsetsWithIndex = A_rowOffset.zipWithIndex().map { case (off, idx) => (idx.toInt, off) }
    val aNextOffsets = aOffsetsWithIndex.map { case (idx, off) => (idx - 1, off) }.filter(_._1 >= 0)

    val aRowRanges = aOffsetsWithIndex
      .join(aNextOffsets)
      .map { case (rowId, (start, end)) => (rowId, (start, end)) }
    val aPosToRow = aRowRanges.flatMap { case (rowId, (start, end)) =>
      (start until end).map(pos => (pos, rowId))
    }
    val aIndexedCols = A_colIndices.zipWithIndex().map { case (col, idx) => (idx.toInt, col) }
    val aIndexedVals = A_values.zipWithIndex().map { case (v, idx) => (idx.toInt, v) }
    val aByRow = aPosToRow.join(aIndexedCols).join(aIndexedVals).map{
      case (_, ((i, c), v)) => (c, (i, v))
    }

    //B_metrix processing
    val bByCol = B_matrix.zipWithIndex().flatMap{
      case (row, i) =>
        row.zipWithIndex.map{
          case (v, j) =>
            (i.toInt, (j.toInt, v))
        }
    }

    //Calculate AxB
    val result = aByRow.join(bByCol).map{
      case (c, ((i, av), (j, bv)))=>
        ((i, j), av*bv)
    }

    result.reduceByKey(_ + _).map{
      case ((c, j), v) =>
        (c, j, v)
    }
  }
}
