/*
运行PDD时会输出很多红色日志，目前我还没找到把它关闭的方法。
运行前需要注意JAVA CDK的版本，最好使用JAVA8或者JAVA11
*/

object ReadCSV {
  def main(args: Array[String]): Unit = {
    // Set log level
//    org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.ERROR)
//    org.apache.log4j.Logger.getLogger("akka").setLevel(org.apache.log4j.Level.ERROR)

    val Con = new Converter

//    val filePath1 = "D:\\EdinburghUniversity\\Git_Project\\PDSS_CW1\\Data\\SMConverter_test.csv"
//    println("The file path is: " + filePath1)
//
//    //测试COO转换
//    val (row1, col1, value1, size1) = Con.SMToCOO(filePath1)
//    println("Read the Sparse matrix in file and saved in COO form:")
//    println("row:   ")
//    row1.toLocalIterator.foreach(println)
//    println("col:   ")
//    col1.toLocalIterator.foreach(println)
//    println("value:  ")
//    value1.toLocalIterator.foreach(println)
//    println("The size of SM is:")
//    println(size1)
//
//
//    //测试CSR转换
//    val (row2, col2, value2, size2) = Con.SMToCSR(filePath1)
//    println("Read the Sparse matrix in file and saved in CSR form:")
//    println("rowOffset:   ")
//    row2.toLocalIterator.foreach(println)
//    println("col:   ")
//    col2.toLocalIterator.foreach(println)
//    println("value:  ")
//    value2.toLocalIterator.foreach(println)
//    println("The size of SM is:")
//    println(size2)
//
//    //测试CSC转换
//    val (row3, col3, value3, size3) = Con.SMToCSC(filePath1)
//    println("Read the Sparse matrix in file and saved in CSR form:")
//    println("row:   ")
//    row3.toLocalIterator.foreach(println)
//    println("colOffset:   ")
//    col3.toLocalIterator.foreach(println)
//    println("value:  ")
//    value3.toLocalIterator.foreach(println)
//    println("The size of SM is:")
//    println(size3)
//
//    //测试SELL转换
//    val (row4, col4, value4, size4) = Con.SMToSELL(filePath1, 2)
//    println("Read the Sparse matrix in file and saved in SELL form:")
//    println("Slice:   ")
//    row4.toLocalIterator.foreach(println)
//    println("colOffset:   ")
//    col4.toLocalIterator.foreach(println)
//    println("value:  ")
//    value4.toLocalIterator.foreach(println)
//    println("The size of SM is:")
//    println(size4)
//
    //测试SV读取
    val filePath2 = "D:\\EdinburghUniversity\\Git_Project\\PDSS_CW1\\Data\\SVConverter_test.csv"
    println("The file path is: " + filePath2)
    val (ind, value, size) = Con.ReadSV(filePath2)
    println("idices:  ")
    ind.toLocalIterator.foreach{
      indx =>
        println(indx)
    }
    println("value:  ")
    value.toLocalIterator.foreach{
      valus =>
        println(valus)
    }
    println("The size of SV is:")
    println(size)
//
//    val filePath3 = "D:\\EdinburghUniversity\\Git_Project\\PDSS_CW1\\Data\\DVConverter_test.csv"
//    println("The file path is: " + filePath3)
//
//    //测试DV读取
//    val (value5, size5) = Con.ReadDV(filePath3)
//    println("The value in the vector:")
//    value5.toLocalIterator.foreach(println)
//    println("The size of the vector is:")
//    println(size5)
//
//    val filePath4 = "D:\\EdinburghUniversity\\Git_Project\\PDSS_CW1\\Data\\DMConverter_test.csv"
//    println("The file path is: " + filePath4)
//
//    //测试DM读取
//    val (value6, size6) = Con.ReadDM(filePath4)
//    println("The value in the vector:")
//    value6.toLocalIterator.foreach{
//      line =>
//        println(line.mkString(","))
//    }
//    println("The size of the vector is:")
//    println(size6)
  }
}
