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

  def SMToCOO (address: String): (RDD[Int], RDD[Int], RDD[Double]) = {
    /*
    Row:       ,List(0, 0, 1, 3)
    Col:       ,List(0, 2, 1, 3)
    Value:     ,List(4.0, 9.0, 7.0, 5.0)
     */
    var rowIndices = List[Int]()
    var colIndices = List[Int]()
    var values = List[Double]()
    val source = Source.fromFile(address)
    try {
      val lines = source.getLines().toList

      for ((line, i) <- lines.zipWithIndex) {
        val entries = line.replace("\uFEFF", "").split(",").map(_.trim.toDouble)
        for ((v, j) <- entries.zipWithIndex) {
          if (v != 0) {
            rowIndices ::= i
            colIndices ::= j
            values ::= v
          }
        }
      }
    } finally {
      source.close()
    }
    println("Row:       ", rowIndices.reverse)
    println("Col:       ", colIndices.reverse)
    println("Value:     ", values.reverse)
    (sc.parallelize(rowIndices.reverse), sc.parallelize(colIndices.reverse), sc.parallelize(values.reverse))

  }

  def SMToCSC (address: String): (RDD[Int], RDD[Int], RDD[Double]) = {
    /*
    Row:       ,List(0, 1, 0, 3)
    ColOffset: ,List(0, 1, 2, 3, 4)
    Value:     ,List(4, 7, 9, 5)
     */
    var rowIndices = List[Int]()
    var colOffset = List[Int]()
    var values = List[Double]()
    var indices: Int = 0
    val source = Source.fromFile(address)
    val matrix = source.getLines().map(_.replace("\uFEFF", "").split(",").map(_.trim.toDouble)).toArray

    val numOFrows = matrix.length
    val numOFcol = matrix(0).length
    try {
      for (i <- 0 until  numOFcol){
        colOffset ::= indices
        for (j <- 0 until numOFrows){
          if (matrix(j)(i) != 0){
            indices += 1
            rowIndices ::= j
            values ::= matrix(j)(i)
          }
        }
      }
      colOffset ::= indices
    } finally {
      source.close()
    }
    println("Row:       ", rowIndices.reverse)
    println("Col:       ", colOffset.reverse)
    println("Value:     ", values.reverse)
    (sc.parallelize(rowIndices.reverse), sc.parallelize(colOffset.reverse), sc.parallelize(values.reverse))
  }

  def SMToCSR (address: String): (RDD[Int], RDD[Int], RDD[Double]) = {
    /*
    RowOffset: ,List(0, 2, 3, 3, 4)
    Col:       ,List(0, 2, 1, 3)
    Value:     ,List(4.0, 9.0, 7.0, 5.0)
     */
    var rowOffset = List[Int]()
    var colIndices = List[Int]()
    var values = List[Double]()
    var indices: Int = 0
    val source = Source.fromFile(address)

    try {
      val lines = source.getLines().toList

      for ((line, i) <- lines.zipWithIndex) {
        rowOffset ::= indices
        val entries = line.replace("\uFEFF", "").split(",").map(_.trim.toDouble)
        for ((v, j) <- entries.zipWithIndex) {
          if (v != 0) {
            colIndices ::= j
            values ::= v
            indices += 1
          }
        }
      }
      rowOffset ::= indices
    } finally {
      source.close()
    }
    println("Row:       ", rowOffset.reverse)
    println("Col:       ", colIndices.reverse)
    println("Value:     ", values.reverse)
    (sc.parallelize(rowOffset.reverse), sc.parallelize(colIndices.reverse), sc.parallelize(values.reverse))
  }

  def SMToSELL (address: String, sliceHigh: Int): (RDD[Int], RDD[Int], RDD[Double]) = {
    /*
    sliceHigh = 2
    sliceNum:  ,List(0, 4, 6)
    Col:       ,List(0, 1, 2,-1,-1, 3)
    Value:     ,List(4, 7, 9, *, *, 5)
     */
    val source = Source.fromFile(address)
    val matrix = source.getLines().map(_.replace("\uFEFF", "").split(",").map(_.trim.toDouble)).toArray

    val numOFrows = matrix.length
    val numOFcol = matrix(0).length
    val numOFslice = (numOFrows + sliceHigh - 1) / sliceHigh

    var values = List[Double]()
    var colIdices = List[Int]()
    var sliceNum = List[Int]()
    sliceNum ::= 0

    for (i <- 0 until numOFslice){
      val start = i * sliceHigh
      val end = Math.min(start + sliceHigh, numOFrows)
      val slice = matrix.slice(start, end)
      val MaxWith = slice.map(_.count(_ != 0)).max

      for (j <- slice){
        val nz = j.zipWithIndex.filter(_._1 != 0)
        val res = MaxWith - nz.length
        values = values ++ (nz.map(_._1) ++ List.fill(res)(0.0))
        colIdices = colIdices ++ (nz.map(_._2) ++ List.fill(res)(-1))
      }
      sliceNum ::= values.length
    }

    println("Slice:     ", sliceNum.reverse)
    println("Col:       ", colIdices)
    println("Value:     ", values)

    (sc.parallelize(sliceNum.reverse), sc.parallelize(colIdices), sc.parallelize(values))
  }

    def ReadSV (address: String): (RDD[Int], RDD[Double]) = {
      /*
      Idices:    ,List(1, 4, 5, 9)
      values:    ,List(1, 4, 7, 4)
       */
      val source = Source.fromFile(address)
      val matrix = source.getLines().map(_.replace("\uFEFF", "").split(",").map(_.trim.toDouble)).toArray
      val transposed = matrix.transpose
      val numOFrows = matrix.length

      var values = List[Double]()
      var Idices = List[Int]()

      if (numOFrows != 1){
        println("The input is not a vector.")
        return (sc.parallelize(Idices), sc.parallelize(values.reverse))
      }

      val nz = transposed.zipWithIndex.filter(a => a._1.exists(_ != 0))
      values = values ++ (nz.map(_._1).flatten)
      Idices = Idices ++ (nz.map(_._2))

      println("Col:       ", Idices)
      println("Value:     ", values)

      (sc.parallelize(Idices), sc.parallelize(values.reverse))
    }
}
