import org.apache.spark.SparkContext

/* 新
运行PDD时会输出很多红色日志，目前我还没找到把它关闭的方法。
运行前需要注意JAVA CDK的版本，最好使用JAVA8或者JAVA11
*/

object Tester {
  def main(args: Array[String]): Unit = {
    // ----------------------------------------------------
    // [ 修复 ] 1. 正确的初始化顺序
    // ----------------------------------------------------
    // a. 创建 Runner 来启动 SC
    val Run = new Runner
    // b. 获取 SC 并使其在 main 作用域中可见且隐式
    implicit val sc: SparkContext = Run.sc

    // c. 现在 Converter 和 Calculator 会自动找到隐式的 sc
    val Con = new Converter
    val Cal = new Calculator
    // ---------------------------------

    println("=================================================")
    println("      PDSS CW1 单元测试开始 (纯 RDD 模式)")
    println(s"SparkContext 运行在: ${sc.master}")
    println("=================================================")

    // --- 2. 加载所有 RDDs ---
    val SM_rdd = sc.textFile("Data/SMConverter_test.csv")
    val SV_rdd = sc.textFile("Data/SVConverter_test.csv")
    val DV_rdd = sc.textFile("Data/DVConverter_test.csv")
    val DM_rdd = sc.textFile("Data/DMConverter_test.csv")
    val SMSM_rdd = sc.textFile("Data/SMSM_test.csv")
    val SMDM_rdd = sc.textFile("Data/SMDM_test.csv")


    println(s"\n--- [1] 测试 SMToCOO (SMConverter_test.csv) ---")
    // [ 修复 ] 移除了 (sc)
    val (cooRDD, size1) = Con.SMToCOO(SM_rdd)
    println(s"Matrix Size: $size1")
    println("COO Tuples (Row, Col, Val):")
    val cooList = cooRDD.collect()
    cooList.foreach(println)


    println(s"\n--- [2] 测试 SMToJoinableByRow (逻辑 CSR) ---")
    // [ 修复 ] 调用新方法
    val (csrJoinable, sizeCSR) = Con.SMToJoinableByRow(SM_rdd)
    println(s"Matrix Size: $sizeCSR")
    println("Joinable CSR RDD [(Row, (Col, Val))]:")
    csrJoinable.collect().foreach(println)


    println(s"\n--- [3] 测试 SMToJoinableByCol (逻辑 CSC) ---")
    // [ 修复 ] 调用新方法
    val (cscJoinable, sizeCSC) = Con.SMToJoinableByCol(SM_rdd)
    println(s"Matrix Size: $sizeCSC")
    println("Joinable CSC RDD [(Col, (Row, Val))]:")
    cscJoinable.collect().foreach(println)


    println(s"\n--- [4] 测试 ReadSV (SVConverter_test.csv) ---")
    val (ind, value, size_sv) = Con.ReadSV(SV_rdd)
    println(s"Vector Length: $size_sv")
    println("SV Indices: " + ind.collect().mkString(","))
    println("SV Values: " + value.collect().mkString(","))


    println(s"\n--- [5] 测试 ReadDV (DVConverter_test.csv) ---")
    val (value5, size5) = Con.ReadDV(DV_rdd)
    println(s"Vector Length: $size5")
    println("DV Values: " + value5.collect().mkString(","))


    println(s"\n--- [6] 测试 ReadDM (DMConverter_test.csv) ---")
    val (value6, size6) = Con.ReadDM(DM_rdd)
    println(s"Matrix Size: $size6")
    println("DM Rows:")
    value6.collect().foreach(row => println(s"  [${row.mkString(", ")}]"))


    println("\n--- [7] 测试 SpM_DV (CSR x 稠密向量) ---")
    // [ 修复 ] 使用新的 "Joinable" RDDs
    val (csr_rdd, csr_shape) = Con.SMToJoinableByRow(SM_rdd)
    val (dv_rdd, dv_len) = Con.ReadDV(DV_rdd)
    val result = Cal.SpM_DV(csr_rdd, dv_rdd, csr_shape)
    println("Result Vector:")
    println(result.collect().mkString(","))


    println("\n--- [8] 测试 SpM_SpSV (CSR x 稀疏向量) ---")
    val (csr_rdd_2, csr_shape_2) = Con.SMToJoinableByRow(SM_rdd)
    val (svIndices, svValues, vecLength) = Con.ReadSV(SV_rdd)
    val result2 = Cal.SpM_SpSV(csr_rdd_2, svIndices, svValues, csr_shape_2, vecLength)
    println("Result Vector:")
    println(result2.collect().mkString(","))


    println("\n--- [9] 测试 SpM_SpM (CSR x CSC) ---")
    // [ 修复 ] 使用新的 "Joinable" RDDs
    val (a_byRow, a_shape) = Con.SMToJoinableByRow(SM_rdd)
    val (b_byCol, b_shape) = Con.SMToJoinableByCol(SMSM_rdd)
    val result3 = Cal.SpM_SpM(a_byRow, b_byCol, a_shape, b_shape)
    println("Result Matrix in COO:")
    result3.collect().foreach(println)


    println("\n--- [10] 测试 SpM_SpDM (CSR x 稠密矩阵) ---")
    // [ 修复 ] 使用新的 "Joinable" RDDs
    val (a_byRow_2, a_shape_2) = Con.SMToJoinableByRow(SM_rdd)
    val (b_dense_rdd, b_dense_shape) = Con.ReadDM(SMDM_rdd)
    val result4 = Cal.SpM_SpDM(a_byRow_2, b_dense_rdd, a_shape_2, b_dense_shape)
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

//import org.apache.spark.{SparkConf, SparkContext}
//
///* 新
//运行PDD时会输出很多红色日志，目前我还没找到把它关闭的方法。
//运行前需要注意JAVA CDK的版本，最好使用JAVA8或者JAVA11
//*/
//
//object Tester {
//  def main(args: Array[String]): Unit = {
//    // 设置日志级别 (注意：最好的方法是使用 src/main/resources/log4j2.properties 文件)
//    org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.ERROR)
//    org.apache.log4j.Logger.getLogger("akka").setLevel(org.apache.log4j.Level.ERROR)
//
//    // --- SC 在 Converter 内部启动 ---
//    val Con = new Converter
//    val Cal = new Calculator
//    val Run = new Runner
//    val sc = Run.sc
//    // ---------------------------------
//
//    println("=================================================")
//    println("          PDSS CW1 单元测试开始")
//    println(s"SparkContext 运行在: ${sc.master}")
//    println("=================================================")
//
//
//    val filePath1 = "Data/SMConverter_test.csv"
//    println(s"\n--- [1] 测试 SMToCOO ($filePath1) ---")
//
//    // [ 修复 ] SMToCOO 返回 (RDD[(Int, Int, Double)], (Int, Int))，是 2 个值, 而不是 4 个
//    val SM = sc.textFile(filePath1)
//    val (cooRDD, size1) = Con.SMToCOO(SM)(sc)
//    println(s"Matrix Size: $size1")
//    println("COO Tuples (Row, Col, Val):")
//    // .collect() 是安全的，因为这是单元测试，数据很小
//    val cooList = cooRDD.collect()
//    cooList.foreach(println)
//
//
//    println(s"\n--- [2] 测试 SMToCSR ($filePath1) ---")
//    val (row2, col2, value2, size2) = Con.SMToCSR(SM)(sc)
//    println(s"Matrix Size: $size2")
//    // 使用 .collect() 和 .mkString() 使输出更整洁
//    println("CSR rowOffset: " + row2.collect().mkString(","))
//    println("CSR colIndices: " + col2.collect().mkString(","))
//    println("CSR values: " + value2.collect().mkString(","))
//
//
//    println(s"\n--- [3] 测试 SMToCSC ($filePath1) ---")
//    val (row3, col3, value3, size3) = Con.SMToCSC(SM)(sc)
//    println(s"Matrix Size: $size3")
//    println("CSC rowIndices: " + row3.collect().mkString(","))
//    println("CSC colOffset: " + col3.collect().mkString(","))
//    println("CSC values: " + value3.collect().mkString(","))
//
//
//    println(s"\n--- [4] 测试 ReadSV (SVConverter_test.csv) ---")
//    val filePath2 = "Data/SVConverter_test.csv"
//    val SV = sc.textFile(filePath2)
//    val (ind, value, size_sv) = Con.ReadSV(SV)(sc)
//    println(s"Vector Length: $size_sv")
//    println("SV Indices: " + ind.collect().mkString(","))
//    println("SV Values: " + value.collect().mkString(","))
//
//
//    println(s"\n--- [5] 测试 ReadDV (DVConverter_test.csv) ---")
//    val filePath3 = "Data/DVConverter_test.csv"
//    val DV = sc.textFile(filePath3)
//    val (value5, size5) = Con.ReadDV(DV)(sc)
//    println(s"Vector Length: $size5")
//    println("DV Values: " + value5.collect().mkString(","))
//
//
//    println(s"\n--- [6] 测试 ReadDM (DMConverter_test.csv) ---")
//    val filePath4 = "Data/DMConverter_test.csv"
//    val DM = sc.textFile(filePath4)
//    val (value6, size6) = Con.ReadDM(DM)(sc)
//    println(s"Matrix Size: $size6")
//    println("DM Rows:")
//    value6.collect().foreach(row => println(s"  [${row.mkString(", ")}]"))
//
//
//    println("\n--- [7] 测试 SpM_DV (CSR x 稠密向量) ---")
//    // (假设您已将 Calculator 中的 csrMultiply 重命名为 SpM_DV)
//    val (rowOffset, colIndices, values, shape) = Con.SMToCSR(SM)(sc)
//    val (vector, n) = Con.ReadDV(DV)(sc)
//    val result = Cal.SpM_DV(rowOffset, colIndices, values, vector, shape)(sc)
//    println("Result Vector:")
//    println(result.collect().mkString(","))
//
//
//    println("\n--- [8] 测试 SpM_SpSV (CSR x 稀疏向量) ---")
//    val (rowOffset2, colIndices2, values2, shape2) = Con.SMToCSR(SM)(sc)
//    val (svIndices, svValues, vecLength) = Con.ReadSV(SV)(sc)
//    val result2 = Cal.SpM_SpSV(rowOffset2, colIndices2, values2, svIndices, svValues, shape2, vecLength)(sc)
//
//    // [ 修复 ] 删除了 sc.stop()
//
//    println("Result Vector:")
//    println(result2.collect().mkString(","))
//
//
//    println("\n--- [9] 测试 SpM_SpM (CSR x CSC) ---")
//    val (rowOffset3, colIndices3, values3, shape3) = Con.SMToCSR(SM)(sc)
//    val SM2 = sc.textFile("Data/SMSM_test.csv")
//    val (row, colOffset, value4, shape_csc) = Con.SMToCSC(SM2)(sc)
//    val result3 = Cal.SpM_SpM(rowOffset3, colIndices3, values3, colOffset, row, value4, shape3, shape_csc)(sc)
//
//    // [ 修复 ] 删除了 sc.stop()
//
//    println("Result Matrix in COO:")
//    result3.collect().foreach(println)
//
//
//    println("\n--- [10] 测试 SpM_SpDM (CSR x 稠密矩阵) ---")
//    val DM2 = sc.textFile("Data/SMDM_test.csv")
//    val (rowOffset4, colIndices4, values4, shape4) = Con.SMToCSR(SM)(sc)
//    val (matrix, shape5) = Con.ReadDM(DM2)(sc)
//    val result4 = Cal.SpM_SpDM(rowOffset4, colIndices4, values4, matrix, shape4, shape5)(sc)
//
//    // [ 修复 ] 删除了 sc.stop()
//
//    println("Result Matrix in COO:")
//    result4.collect().foreach(println)
//
//
//    // ----------------------------------------------------
//    // [ 修复 ] 在所有测试完成后，只在这里停止一次 SC
//    // ----------------------------------------------------
//    println("\n=================================================")
//    println("            所有测试已完成。")
//    println("=================================================")
//    sc.stop()
//  }
//}