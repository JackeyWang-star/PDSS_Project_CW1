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

//    val filePath1 = "Data/SMConverter_test.csv"
//    println("The file path is: " + filePath1)
//
//    //测试COO转换
//    val (row1, col1, value1, size1) = Con.SMToCOO(filePath1)
//    println("Read the Sparse matrix in file and saved in COO form:")
//    println("row:   ")
//    val Rnum1 = row1.count().toInt
//    val RList1 = row1.take(Rnum1).toList
//    RList1.foreach(println)
//    println("col:   ")
//    val Cnum1 = col1.count().toInt
//    val CList1 = col1.take(Cnum1).toList
//    CList1.foreach(println)
//    println("value:  ")
//    val Vnum1 = value1.count().toInt
//    val VList1 = value1.take(Vnum1).toList
//    VList1.foreach(println)
//    println("The size of SM is:")
//    println(size1)
//
//
//    //测试CSR转换
//    val (row2, col2, value2, size2) = Con.SMToCSR(filePath1)
//    println("Read the Sparse matrix in file and saved in CSR form:")
//    println("rowOffset:   ")
//    val Rnum2 = row2.count().toInt
//    val RList2 = row2.take(Rnum2).toList
//    RList2.foreach(println)
//    println("col:   ")
//    val Cnum2 = col2.count().toInt
//    val CList2 = col2.take(Cnum2).toList
//    CList2.foreach(println)
//    println("value:  ")
//    val Vnum2 = value2.count().toInt
//    val VList2 = value2.take(Vnum2).toList
//    VList2.foreach(println)
//    println("The size of SM is:")
//    println(size2)
//
//    //测试CSC转换
//    val (row3, col3, value3, size3) = Con.SMToCSC(filePath1)
//    println("Read the Sparse matrix in file and saved in CSC form:")
//    println("row:   ")
//    row3.toLocalIterator.foreach(println)
//    println("colOffset:   ")
//    col3.toLocalIterator.foreach(println)
//    println("value:  ")
//    value3.toLocalIterator.foreach(println)
//    println("The size of SM is:")
//    println(size3)
//
//
//    //测试SV读取
//    val filePath2 = "Data/SVConverter_test.csv"
//    println("The file path is: " + filePath2)
//    val (ind, value, size) = Con.ReadSV(filePath2)
//    val long = ind.count().toInt
//    val Index1 = ind.take(long).toList
//    Index1.foreach(println)
//    println("value:  ")
//    val lar = value.count().toInt
//    val values = value.take(lar).toList
//    values.foreach(println)
//    println("The size of SV is:")
//    println(size)
//
//    //测试DV读取
//    val filePath3 = "Data/DVConverter_test.csv"
//    println("The file path is: " + filePath3)
//    val (value5, size5) = Con.ReadDV(filePath3)
//    println("The value in the vector:")
//    val len = value5.count().toInt
//    val va = value5.take(len).toList
//    va.foreach(println)
//    println("The size of the vector is:")
//    println(size5)
//
//    //测试DM读取
//    val filePath4 = "Data/DMConverter_test.csv"
//    println("The file path is: " + filePath4)
//    val (value6, size6) = Con.ReadDM(filePath4)
//    println("The value in the vector:")
//    val lens = value6.count().toInt
//    val vl = value6.take(lens).toList
//    vl.foreach{
//      line =>
//        println(line.mkString(","))
//    }
//    println("The size of the vector is:")
//    println(size6)
  }
}
