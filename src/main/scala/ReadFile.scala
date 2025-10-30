/*
SMToCOO (address: String): (RDD[Int], RDD[Int], RDD[Double])
SMToCSC (address: String): (RDD[Int], RDD[Int], RDD[Double])
SMToCSR (address: String): (RDD[Int], RDD[Int], RDD[Double])
这三个方法包含了读取CSV文件并转换成COO，CSC，CSR这三种保存形式。结构会被保存在三个PDD中返回。
运行PDD时会输出很多红色日志，目前我还没找到把它关闭的方法。
运行前需要注意JAVA CDK的版本，最好使用JAVA8或者JAVA11
*/
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.io.Source
import org.apache.log4j.{Level, Logger}

object ReadCSV {
  def main(args: Array[String]): Unit = {
    // Set log level
    org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.ERROR)
    org.apache.log4j.Logger.getLogger("akka").setLevel(org.apache.log4j.Level.ERROR)

    val Con = new Converter

    val filePath = "D:\\EdinburghUniversity\\Git_Project\\PDSS_CW1\\Data\\SMConverter_test.csv"
    println("The file path is: " + filePath)

    //测试COO转换
    val (row1, col1, value1) = Con.SMToCOO(filePath)
    println("Read the Sparse matrix in file and saved in COO form:")
    println("row:   ")
    row1.toLocalIterator.foreach(println)
    println("col:   ")
    col1.toLocalIterator.foreach(println)
    println("value:  ")
    value1.toLocalIterator.foreach(println)

    //测试CSR转换
    val (row2, col2, value2) = Con.SMToCSR(filePath)
    println("Read the Sparse matrix in file and saved in CSR form:")
    println("rowOffset:   ")
    row2.toLocalIterator.foreach(println)
    println("col:   ")
    col2.toLocalIterator.foreach(println)
    println("value:  ")
    value2.toLocalIterator.foreach(println)

    //测试CSC转换
    val (row3, col3, value3) = Con.SMToCSC(filePath)
    println("Read the Sparse matrix in file and saved in CSR form:")
    println("row:   ")
    row3.toLocalIterator.foreach(println)
    println("colOffset:   ")
    col3.toLocalIterator.foreach(println)
    println("value:  ")
    value3.toLocalIterator.foreach(println)

    //测试SELL转换
    val (row4, col4, value4) = Con.SMToSELL(filePath, 2)
    println("Read the Sparse matrix in file and saved in SELL form:")
    println("Slice:   ")
    row4.toLocalIterator.foreach(println)
    println("colOffset:   ")
    col4.toLocalIterator.foreach(println)
    println("value:  ")
    value4.toLocalIterator.foreach(println)

  }
}
