import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.io.Source

object ReadCSV {
  def SMToCOO (address: String): (List[Int], List[Int], List[Double]) = {
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
    return (rowIndices.reverse, colIndices.reverse, values.reverse)

  }
  def main(args: Array[String]): Unit = {
    val filePath = "D:\\EdinburghUniversity\\Git_Project\\PDSS_CW1\\Data\\SMConverter_test.csv"
    println("The file path is: " + filePath)
    val (row, col, value) = SMToCOO(filePath)
    println("Read the Sparse matrix in file and saved in COO form:")
    println("row:   " + row)
    println("col:   " + col)
    println("value:  " + value)
  }
}