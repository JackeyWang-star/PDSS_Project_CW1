import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.io.Source

class Converter {
  private val conf: SparkConf = new SparkConf()
    .setAppName("CW1") // Set your application's name
    .setMaster("local[*]") // Use all cores of the local machine
    .set("spark.ui.enabled", "false")
  val sc: SparkContext = new SparkContext(conf)

  def SMToCOO (address: String): (RDD[(Int, Int, Double)], (Int, Int)) = {
    /*
    Row:       ,List(0, 0, 1, 3)
    Col:       ,List(0, 2, 1, 3)
    Value:     ,List(4.0, 9.0, 7.0, 5.0)
     */
    val matrix = sc.textFile(address)
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
//    val rowIndex = RDDvalue.map(_._1)
//    val colIndex = RDDvalue.map(_._2)
//    val values = RDDvalue.map(_._3)

    (RDDvalue, (numOFrow, numOFcol))
  }

  //修改后的CSC
  def SMToCSC (address: String): (RDD[Int], RDD[Int], RDD[Double], (Int, Int)) = {
    /*
    Row:       ,List(0, 1, 0, 3)
    ColOffset: ,List(0, 1, 2, 3, 4, 4)
    Value:     ,List(4, 7, 9, 5)
     */
    val (coo, size) = SMToCOO(address)
    val (numRows, numCols) = size

    // 1. 对 COO RDD 按列索引排序 (CSC 格式的必要条件)
    val sortedCOO = coo.sortBy { case (r, c, v) => (c, r) }.persist()

    // 2. 分布式计算每列的非零元素个数
    // (col, 1) -> reduceByKey -> (col, count_for_that_col)
    val colCountsRDD = sortedCOO.map { case (_, c, _) => (c, 1) }
      .reduceByKey(_ + _)

    // 3. 安全地将小型的 "列计数" RDD 收集到驱动程序
    // 这是一个 Map[Int, Int]，大小为 numCols，所以这是安全的
    val colCountsMap = colCountsRDD.collectAsMap()

    // 4. 在驱动程序上构建偏移量数组（处理 numCols 个元素）
    val numList: List[Int] = (0 until numCols).map { index =>
      colCountsMap.getOrElse(index, 0)
    }.toList

    // 5. 计算前缀和 (Prefix Sum) 来创建偏移量
    val resultList = (0 :: numList).foldLeft((List.empty[Int], 0)){
      case ((offset, sum), current) =>
        val newsum = sum + current
        (offset :+ newsum, newsum)
    }._1

    // 6. 将小的偏移量数组并行化回 RDD
    val colOffset = sc.parallelize(resultList)

    // 7. 从已排序的 RDD 中提取行索引和值
    val rowIndexRDD = sortedCOO.map(_._1)
    val valueRDD = sortedCOO.map(_._3)

    // 8. 清除缓存
    sortedCOO.unpersist()

    (rowIndexRDD, colOffset, valueRDD, size)
  }

//  def SMToCSC (address: String): (RDD[Int], RDD[Int], RDD[Double], (Int, Int)) = {
//    /*
//    Row:       ,List(0, 1, 0, 3)
//    ColOffset: ,List(0, 1, 2, 3, 4, 4)
//    Value:     ,List(4, 7, 9, 5)
//     */
//    val (coo, size) = SMToCOO(address)
////    val indexedRow = row.zipWithIndex().map{
////      case (r, i) => (i, r)
////    }
////    val indexedcol = col.zipWithIndex().map{
////      case (c, i) => (i, c)
////    }
////    val indexedValue = value.zipWithIndex().map{
////      case (v, i) => (i, v)
////    }
////    val RowCol = indexedRow.join(indexedcol)
////    val RowColValue = RowCol.join(indexedValue)
//    val cooRDD = coo.map{
//      case (r, c, v) => (r, c, v)
//    }
//    val sortedRDD = cooRDD.sortBy{
//      case (r, c, v) => (c, r)
//    }
//
//    val rowIndexRDD = sortedRDD.map(_._1)
//    val valueRDD = sortedRDD.map(_._3)
//    val CList = coo.map(_._2).take(coo.map(_._2).count().toInt).toList
//    val countMap = CList
//      .groupBy(identity)
//      .map { case (key, value) => (key, value.size) }
//
//    val numList: List[Int] = (0 until size._2).map { index =>
//      countMap.getOrElse(index, 0)
//    }.toList
//
//    val resultList = (0 :: numList).foldLeft((List.empty[Int], 0)){
//      case ((offset, sum), current) =>
//        val newsum = sum + current
//        (offset :+ newsum, newsum)
//    }._1
//
//    val colOffset = sc.parallelize(resultList)
//    (rowIndexRDD, colOffset, valueRDD, size)
//  }

//修改后的CSR
  def SMToCSR (address: String): (RDD[Int], RDD[Int], RDD[Double], (Int, Int)) = {
    /*
    RowOffset: ,List(0, 2, 3, 3, 4)
    Col:       ,List(0, 2, 1, 3)
    Value:     ,List(4.0, 9.0, 7.0, 5.0)
     */
    val (coo, size) = SMToCOO(address)
    val (numRows, numCols) = size

    // 1. 对 COO RDD 按行索引排序
    val sortedCOO = coo.sortBy { case (r, c, v) => (r, c) }.persist()

    // 2. 分布式计算每行的非零元素个数
    // (row, 1) -> reduceByKey -> (row, count_for_that_row)
    val rowCountsRDD = sortedCOO.map { case (r, _, _) => (r, 1) }
      .reduceByKey(_ + _)

    // 3. 安全地将小型的 "行计数" RDD 收集到驱动程序
    //    这是一个 Map[Int, Int]，大小为 numRows，而不是 nnz，所以这是安全的
    val rowCountsMap = rowCountsRDD.collectAsMap()

    // 4. 在驱动程序上构建偏移量数组（现在是安全的，因为只处理 numRows 个元素）
    val numList: List[Int] = (0 until numRows).map { index =>
      rowCountsMap.getOrElse(index, 0)
    }.toList

    // 5. 计算前缀和 (Prefix Sum) 来创建偏移量
    val resultList = (0 :: numList).foldLeft((List.empty[Int], 0)){
      case ((offset, sum), current) =>
        val newsum = sum + current
        (offset :+ newsum, newsum)
    }._1

    // 6. 将小的偏移量数组并行化回 RDD
    val rowOffset = sc.parallelize(resultList)

    // 7. 从已排序的 RDD 中提取列索引和值
    val colRDD = sortedCOO.map(_._2)
    val valueRDD = sortedCOO.map(_._3)

    // 8. 清除缓存
    sortedCOO.unpersist()

    (rowOffset, colRDD, valueRDD, size)
  }

//  def SMToCSR (address: String): (RDD[Int], RDD[Int], RDD[Double], (Int, Int)) = {
//    /*
//    RowOffset: ,List(0, 2, 3, 3, 4)
//    Col:       ,List(0, 2, 1, 3)
//    Value:     ,List(4.0, 9.0, 7.0, 5.0)
//     */
//    val (coo, size) = SMToCOO(address)
//    val RList = coo.map(_._1).take(size._1).toList
//    val countMap = RList
//      .groupBy(identity)
//      .map { case (key, value) => (key, value.size) }
//
//    val numList: List[Int] = (0 until size._1).map { index =>
//      countMap.getOrElse(index, 0)
//    }.toList
//
//    val resultList = (0 :: numList).foldLeft((List.empty[Int], 0)){
//      case ((offset, sum), current) =>
//        val newsum = sum + current
//        (offset :+ newsum, newsum)
//    }._1
//
//    val rowOffset = sc.parallelize(resultList)
//    (rowOffset, coo.map(_._2), coo.map(_._3), size)
//  }

  def ReadSV (address: String): (RDD[Int], RDD[Double], Int) = {
    /*
    Idices:    ,List(1, 4, 5, 9)
    values:    ,List(1, 4, 7, 4)
    */
    val vector = sc.textFile(address)
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

  def ReadDV (address: String): (RDD[Double], Int) = {
    val vector = sc.textFile(address)
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

  def ReadDM (address: String): (RDD[Array[Double]], (Int, Int)) = {
    val matrix = sc.textFile(address)
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
