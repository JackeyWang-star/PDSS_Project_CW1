/*
SMToCOO (address: String): (RDD[Int], RDD[Int], RDD[Double])
SMToCSC (address: String): (RDD[Int], RDD[Int], RDD[Double])
SMToCSR (address: String): (RDD[Int], RDD[Int], RDD[Double])
这三个方法包含了读取CSV文件并转换成COO，CSC，CSR这三种保存形式。结构会被保存在三个PDD中返回。
运行PDD时会输出很多红色日志，目前我还没找到把它关闭的方法。
*/
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.io.Source
import org.apache.log4j.{Level, Logger}

object ReadCSV {
  //Global variables
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

  def main(args: Array[String]): Unit = {
    // Set log level
    org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.ERROR)
    org.apache.log4j.Logger.getLogger("akka").setLevel(org.apache.log4j.Level.ERROR)

    val filePath = "D:\\EdinburghUniversity\\Git_Project\\PDSS_CW1\\Data\\SMConverter_test.csv"
    println("The file path is: " + filePath)
    val (row1, col1, value1) = SMToCOO(filePath)
    println("Read the Sparse matrix in file and saved in COO form:")
    println("row:   ")
    row1.toLocalIterator.foreach(println)
    println("col:   ")
    col1.toLocalIterator.foreach(println)
    println("value:  ")
    value1.toLocalIterator.foreach(println)

    val (row2, col2, value2) = SMToCSR(filePath)
    println("Read the Sparse matrix in file and saved in CSR form:")
    println("rowOffset:   ")
    row2.toLocalIterator.foreach(println)
    println("col:   ")
    col2.toLocalIterator.foreach(println)
    println("value:  ")
    value2.toLocalIterator.foreach(println)
    sc.stop()

    val (row3, col3, value3) = SMToCSC(filePath)
    println("Read the Sparse matrix in file and saved in CSR form:")
    println("row:   ")
    row3.toLocalIterator.foreach(println)
    println("colOffset:   ")
    col3.toLocalIterator.foreach(println)
    println("value:  ")
    value3.toLocalIterator.foreach(println)
    sc.stop()
  }
}
