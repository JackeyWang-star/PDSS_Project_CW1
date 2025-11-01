/*
SMToCOO (address: String): (RDD[Int], RDD[Int], RDD[Double])
SMToCSC (address: String): (RDD[Int], RDD[Int], RDD[Double])
SMToCSR (address: String): (RDD[Int], RDD[Int], RDD[Double])
这三个方法包含了读取CSV文件并转换成COO，CSC，CSR这三种保存形式。结构会被保存在三个PDD中返回。

def SMToSELL (address: String, sliceHigh: Int): (RDD[Int], RDD[Int], RDD[Double])
这个方法是转换成SELL格式的方法，但是这个版本保存出来的结果并不正确，现在会按照row的先后顺序保存数据，但实际上应该按照col的顺序

所有格式转换的方法已经被封装进Converter类中，使用时需要先创建Converter实例再调用方法。

 */

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.io.Source

class Converter {
  private val conf: SparkConf = new SparkConf()
    .setAppName("CW1") // Set your application's name
    .setMaster("local[*]") // Use all cores of the local machine
    .set("spark.ui.enabled", "false")
  val sc: SparkContext = new SparkContext(conf)

  def SMToCOO (address: String): (RDD[Int], RDD[Int], RDD[Double], (Int, Int)) = {
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
    val rowIndex = RDDvalue.map(_._1)
    val colIndex = RDDvalue.map(_._2)
    val values = RDDvalue.map(_._3)

    (rowIndex, colIndex, values, (numOFrow, numOFcol))
  }

  def SMToCSC (address: String): (RDD[Int], RDD[Int], RDD[Double], (Int, Int)) = {
    /*
    Row:       ,List(0, 1, 0, 3)
    ColOffset: ,List(0, 1, 2, 3, 4, 4)
    Value:     ,List(4, 7, 9, 5)
     */
    val (row, col, value, size) = SMToCOO(address)
    val cooRDD = row.zip(col).zip(value).map{
      case ((r, c), v) => (r, c, v)
    }
    val sortedRDD = cooRDD.sortBy{
      case (r, c, v) => (c, r)
    }

    val rowIndexRDD = sortedRDD.map(_._1)
    val valueRDD = sortedRDD.map(_._3)
    val CList = col.take(size._2).toList
    val countMap = CList
      .groupBy(identity)
      .map { case (key, value) => (key, value.size) }

    val numList: List[Int] = (0 until size._2).map { index =>
      countMap.getOrElse(index, 0)
    }.toList

    val resultList = (0 :: numList).foldLeft((List.empty[Int], 0)){
      case ((offset, sum), current) =>
        val newsum = sum + current
        (offset :+ newsum, newsum)
    }._1

    val colOffset = sc.parallelize(resultList)
    (rowIndexRDD, colOffset, valueRDD, size)
  }

  def SMToCSR (address: String): (RDD[Int], RDD[Int], RDD[Double], (Int, Int)) = {
    /*
    RowOffset: ,List(0, 2, 3, 3, 4)
    Col:       ,List(0, 2, 1, 3)
    Value:     ,List(4.0, 9.0, 7.0, 5.0)
     */
    val (row, col, value, size) = SMToCOO(address)
    val RList = row.take(size._1).toList
    val countMap = RList
      .groupBy(identity)
      .map { case (key, value) => (key, value.size) }

    val numList: List[Int] = (0 until size._1).map { index =>
      countMap.getOrElse(index, 0)
    }.toList

    val resultList = (0 :: numList).foldLeft((List.empty[Int], 0)){
      case ((offset, sum), current) =>
        val newsum = sum + current
        (offset :+ newsum, newsum)
    }._1

    val rowOffset = sc.parallelize(resultList)
    (rowOffset, col, value, size)
  }

//  def SMToSELL (address: String, sliceHigh: Int): (RDD[Int], RDD[Int], RDD[Double], (Int, Int)) = {
//    /*
//    sliceHigh = 2
//    sliceNum:  ,List(0, 4, 6)
//    Col:       ,List(0, 1, 2,-1,-1, 3)
//    Value:     ,List(4, 7, 9, *, *, 5)
//     */
//    val source = Source.fromFile(address)
//    val matrix = source.getLines().map(_.replace("\uFEFF", "").split(",").map(_.trim.toDouble)).toArray
//
//    val numOFrows = matrix.length
//    val numOFcol = matrix(0).length
//    val numOFslice = (numOFrows + sliceHigh - 1) / sliceHigh
//
//    var values = List[Double]()
//    var colIdices = List[Int]()
//    var sliceNum = List[Int]()
//    sliceNum ::= 0
//
//    for (i <- 0 until numOFslice){
//      val start = i * sliceHigh
//      val end = Math.min(start + sliceHigh, numOFrows)
//      val slice = matrix.slice(start, end)
//      val MaxWith = slice.map(_.count(_ != 0)).max
//
//      for (j <- slice){
//        val nz = j.zipWithIndex.filter(_._1 != 0)
//        val res = MaxWith - nz.length
//        values = values ++ (nz.map(_._1) ++ List.fill(res)(0.0))
//        colIdices = colIdices ++ (nz.map(_._2) ++ List.fill(res)(-1))
//      }
//      sliceNum ::= values.length
//    }
//
//    println("Slice:     ", sliceNum.reverse)
//    println("Col:       ", colIdices)
//    println("Value:     ", values)
//
//    (sc.parallelize(sliceNum.reverse), sc.parallelize(colIdices), sc.parallelize(values), (numOFrows, numOFcol))
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
