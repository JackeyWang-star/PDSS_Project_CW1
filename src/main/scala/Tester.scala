import org.apache.spark.{SparkConf, SparkContext}

/*
运行PDD时会输出很多红色日志，目前我还没找到把它关闭的方法。
运行前需要注意JAVA CDK的版本，最好使用JAVA8或者JAVA11
*/

object Tester {
  def main(args: Array[String]): Unit = {
    // 设置日志级别 (注意：最好的方法是使用 src/main/resources/log4j2.properties 文件)
    org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.ERROR)
    org.apache.log4j.Logger.getLogger("akka").setLevel(org.apache.log4j.Level.ERROR)

    // --- SC 在 Converter 内部启动 ---
    val Con = new Converter
    val Cal = new Calculator
    val sc = Con.sc
    // ---------------------------------

    println("=================================================")
    println("          PDSS CW1 单元测试开始")
    println(s"SparkContext 运行在: ${sc.master}")
    println("=================================================")


    val filePath1 = "Data/SMConverter_test.csv"
    println(s"\n--- [1] 测试 SMToCOO ($filePath1) ---")

    // [ 修复 ] SMToCOO 返回 (RDD[(Int, Int, Double)], (Int, Int))，是 2 个值, 而不是 4 个
    val (cooRDD, size1) = Con.SMToCOO(filePath1)
    println(s"Matrix Size: $size1")
    println("COO Tuples (Row, Col, Val):")
    // .collect() 是安全的，因为这是单元测试，数据很小
    val cooList = cooRDD.collect()
    cooList.foreach(println)


    println(s"\n--- [2] 测试 SMToCSR ($filePath1) ---")
    val (row2, col2, value2, size2) = Con.SMToCSR(filePath1)
    println(s"Matrix Size: $size2")
    // 使用 .collect() 和 .mkString() 使输出更整洁
    println("CSR rowOffset: " + row2.collect().mkString(","))
    println("CSR colIndices: " + col2.collect().mkString(","))
    println("CSR values: " + value2.collect().mkString(","))


    println(s"\n--- [3] 测试 SMToCSC ($filePath1) ---")
    val (row3, col3, value3, size3) = Con.SMToCSC(filePath1)
    println(s"Matrix Size: $size3")
    println("CSC rowIndices: " + row3.collect().mkString(","))
    println("CSC colOffset: " + col3.collect().mkString(","))
    println("CSC values: " + value3.collect().mkString(","))


    println(s"\n--- [4] 测试 ReadSV (SVConverter_test.csv) ---")
    val filePath2 = "Data/SVConverter_test.csv"
    val (ind, value, size_sv) = Con.ReadSV(filePath2)
    println(s"Vector Length: $size_sv")
    println("SV Indices: " + ind.collect().mkString(","))
    println("SV Values: " + value.collect().mkString(","))


    println(s"\n--- [5] 测试 ReadDV (DVConverter_test.csv) ---")
    val filePath3 = "Data/DVConverter_test.csv"
    val (value5, size5) = Con.ReadDV(filePath3)
    println(s"Vector Length: $size5")
    println("DV Values: " + value5.collect().mkString(","))


    println(s"\n--- [6] 测试 ReadDM (DMConverter_test.csv) ---")
    val filePath4 = "Data/DMConverter_test.csv"
    val (value6, size6) = Con.ReadDM(filePath4)
    println(s"Matrix Size: $size6")
    println("DM Rows:")
    value6.collect().foreach(row => println(s"  [${row.mkString(", ")}]"))


    println("\n--- [7] 测试 SpM_DV (CSR x 稠密向量) ---")
    // (假设您已将 Calculator 中的 csrMultiply 重命名为 SpM_DV)
    val (rowOffset, colIndices, values, shape) = Con.SMToCSR("Data/SMConverter_test.csv")
    val (vector, n) = Con.ReadDV("Data/DVConverter_test.csv")
    val result = Cal.SpM_DV(rowOffset, colIndices, values, vector, shape)(sc)
    println("Result Vector:")
    println(result.collect().mkString(","))


    println("\n--- [8] 测试 SpM_SpSV (CSR x 稀疏向量) ---")
    val (rowOffset2, colIndices2, values2, shape2) = Con.SMToCSR("Data/SMConverter_test.csv")
    val (svIndices, svValues, vecLength) = Con.ReadSV("Data/SVConverter_test.csv")
    val result2 = Cal.SpM_SpSV(rowOffset2, colIndices2, values2, svIndices, svValues, shape2, vecLength)(sc)

    // [ 修复 ] 删除了 sc.stop()

    println("Result Vector:")
    println(result2.collect().mkString(","))


    println("\n--- [9] 测试 SpM_SpM (CSR x CSC) ---")
    val (rowOffset3, colIndices3, values3, shape3) = Con.SMToCSR("Data/SMConverter_test.csv")
    val (row, colOffset, value4, shape_csc) = Con.SMToCSC("Data/SMSM_test.csv")
    val result3 = Cal.SpM_SpM(rowOffset3, colIndices3, values3, colOffset, row, value4, shape3, shape_csc)(sc)

    // [ 修复 ] 删除了 sc.stop()

    println("Result Matrix in COO:")
    result3.collect().foreach(println)


    println("\n--- [10] 测试 SpM_SpDM (CSR x 稠密矩阵) ---")
    val (rowOffset4, colIndices4, values4, shape4) = Con.SMToCSR("Data/SMConverter_test.csv")
    val (matrix, shape5) = Con.ReadDM("Data/SMDM_test.csv")
    val result4 = Cal.SpM_SpDM(rowOffset4, colIndices4, values4, matrix, shape4, shape5)(sc)

    // [ 修复 ] 删除了 sc.stop()

    println("Result Matrix in COO:")
    result4.collect().foreach(println)


    // ----------------------------------------------------
    // [ 修复 ] 在所有测试完成后，只在这里停止一次 SC
    // ----------------------------------------------------
    println("\n=================================================")
    println("            所有测试已完成。")
    println("=================================================")
    sc.stop()
  }
}