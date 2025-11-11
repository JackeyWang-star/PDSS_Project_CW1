import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

class Converter(implicit sc: SparkContext) {

  // SMToCOO 保持不变 (它已经是 100% 并行的)
  def SMToCOO (matrix: RDD[String]): (RDD[(Int, Int, Double)], (Int, Int)) = {
    val numOFrow = matrix.count().toInt
    val numOFcol = matrix.first().split(",").length
    val indexMatrix = matrix.zipWithIndex().map{
      case (line, rowindex) => (line, rowindex.toInt)
    }
    val RDDvalue = indexMatrix.flatMap{
      case (line,rowindex) =>
        val ele = line.split(",").map(_.toDouble)
        ele.zipWithIndex.flatMap { case (value, colIndex) =>
          if (value != 0.0) Some((rowindex, colIndex, value))
          else None
        }
    }
    (RDDvalue, (numOFrow, numOFcol))
  }

  // --- 新的纯 RDD 转换器 ---
  // 100% 并行
  def SMToJoinableByRow(matrix: RDD[String]): (RDD[(Int, (Int, Double))], (Int, Int)) = {

    val (coo, size) = SMToCOO(matrix)
    // 按行排序 (逻辑 CSR)
    val sortedCOO = coo.sortBy { case (r, c, _) => (r, c) }
    // 重新键控 (Re-key) 为 RDD[(r, (c, v))]
    val rddByRow = sortedCOO.map { case (r, c, v) => (r, (c, v)) }
    (rddByRow, size)
  }

  // --- 新的纯 RDD 转换器 ---
  def SMToJoinableByCol(matrix: RDD[String]): (RDD[(Int, (Int, Double))], (Int, Int)) = {

    val (coo, size) = SMToCOO(matrix)
    // 按列排序 (逻辑 CSC)
    val sortedCOO = coo.sortBy { case (r, c, _) => (c, r) }
    // 重新键控 (Re-key) 为 RDD[(c, (r, v))]
    val rddByCol = sortedCOO.map { case (r, c, v) => (c, (r, v)) }
    (rddByCol, size)
  }

  // --- Read* 方法保持不变 ---

  def ReadSV (vector: RDD[String]): (RDD[Int], RDD[Double], Int) = {
    val numOFrow = vector.count()
    val numOFcol = vector.first().split(",").length
    if (numOFrow != 1){
      println("The input is not a vector.")
      return (sc.parallelize(List.empty[Int]), sc.parallelize(List.empty[Double]), 0)
    }
    val vectorArr = vector.map{
      values =>
        val ele = values.split(",").map(_.toDouble)
        val nonele = for {
          index <- ele.indices
          value = ele(index)
          if value != 0.0
        } yield (index, value)
        nonele
    }.flatMap(identity)
    ((vectorArr.map(_._1)), (vectorArr.map(_._2)), numOFcol)
  }

  def ReadDV (vector: RDD[String]): (RDD[Double], Int) = {
    val numOFrow = vector.count()
    if (numOFrow != 1){
      println("The input is not a vector.")
      return (sc.parallelize(List.empty[Double]), 0)
    }
    val vectorArr = vector.first().split(",").map(_.toDouble)
    val vectorRDD = sc.parallelize(vectorArr)
    val numOFcol = vectorArr.length
    (vectorRDD, numOFcol)
  }

  def ReadDM (matrix:RDD[String]): (RDD[Array[Double]], (Int, Int)) = {
    val numOFrow = matrix.count().toInt
    if (numOFrow == 0) {
      println("The input matrix is empty.")
      return (sc.parallelize(Seq.empty[Array[Double]]),(0, 0))
    }
    val RDDMatrix = matrix.map{
      line =>
        line.split(",").map(_.toDouble)
    }
    val numOFcol = RDDMatrix.first().length
    (RDDMatrix, (numOFrow,numOFcol))
  }
}