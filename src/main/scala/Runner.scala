import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import java.io._

class Runner {
  // 1. SC 和类在 Runner 内部创建
  private val conf: SparkConf = new SparkConf()
    .setAppName("CW1") // Set your application's name
    .setMaster("local[*]") // Use all cores of the local machine
    .set("spark.ui.enabled", "false")

  // 2. [ 修复 ] 声明 sc 为 implicit，这样 Con 和 Cal 才能自动接收它
  implicit val sc: SparkContext = new SparkContext(conf)

  // 3. [ 修复 ] Con 和 Cal 现在会接收到隐式的 sc
  val Cal = new Calculator
  val Con = new Converter
  val savelink = "result/result.text"

  def Run (address1: String, address2: String): String = {
    // 载入 RDDs
    val SM_rdd = sc.textFile(address1)
    val file_rdd = sc.textFile(address2)

    // 检查文件2 (file_rdd) 的属性
    val numOFrow = file_rdd.count().toInt
    val numOFcol = file_rdd.first().split(",").length
    val size = numOFrow * numOFcol

    if (numOFrow == 1){ // 它是 向量 (Vector)
      if (isS(file_rdd, size)){ // 它是 稀疏向量 (Sparse Vector)
        println("Running SpM_SpSV (Sparse Matrix x Sparse Vector)...")

        // [ 修复 ] 1: 调用新的 SMToJoinableByRow
        val (matrixByRow, shape) = Con.SMToJoinableByRow(SM_rdd)
        // [ 修复 ] 2: 传递 RDD[String]
        val (svIndices, svValues, vecLength) = Con.ReadSV(file_rdd)
        // [ 修复 ] 3: 调用新的 SpM_SpSV
        val result = Cal.SpM_SpSV(matrixByRow, svIndices, svValues, shape, vecLength)

        // [ 修复 ] 收集小向量结果
        val resultString: String = result.collect().mkString(",")
        printV(result)
        return resultString
      }
      else { // 它是 稠密向量 (Dense Vector)
        println("Running SpM_DV (Sparse Matrix x Dense Vector)...")

        // [ 修复 ] 1: 调用 SMToJoinableByRow
        val (matrixByRow, shape) = Con.SMToJoinableByRow(SM_rdd)
        // [ 修复 ] 2: 传递 RDD[String]
        val (vector, n) = Con.ReadDV(file_rdd)
        // [ 修复 ] 3: 调用新的 SpM_DV
        val result = Cal.SpM_DV(matrixByRow, vector, shape)

        // [ 修复 ] 收集小向量结果
        val resultString: String = result.collect().mkString(",")
        printV(result)
        return resultString
      }
    }
    if (numOFrow > 1){ // 它是 矩阵 (Matrix)
      if (isS(file_rdd, size)){ // 它是 稀疏矩阵 (Sparse Matrix)
        println("Running SpM_SpM (Sparse Matrix x Sparse Matrix)...")

        // [ 修复 ] 1: A (CSR) 使用 JoinableByRow
        val (a_matrixByRow, a_shape) = Con.SMToJoinableByRow(SM_rdd)
        // [ 修复 ] 2: B (CSC) 使用 JoinableByCol
        val (b_matrixByCol, b_shape) = Con.SMToJoinableByCol(file_rdd)
        // [ 修复 ] 3: 调用新的 SpM_SpM
        val result = Cal.SpM_SpM(a_matrixByRow, b_matrixByCol, a_shape, b_shape)

        printM(result)
        // [ 修复 ] 4: 使用安全的 saveCOOToString (见下方)
        return saveCOOToString(result)
      }
      else { // 它是 稠密矩阵 (Dense Matrix)
        println("Running SpM_SpDM (Sparse Matrix x Dense Matrix)...")

        // [ 修复 ] 1: A (CSR) 使用 JoinableByRow
        val (a_matrixByRow, a_shape) = Con.SMToJoinableByRow(SM_rdd)
        // [ 修复 ] 2: 传递 RDD[String]
        val (b_matrix, b_shape) = Con.ReadDM(file_rdd)
        // [ 修复 ] 3: 调用新的 SpM_SpDM
        val result = Cal.SpM_SpDM(a_matrixByRow, b_matrix, a_shape, b_shape)

        printM(result)
        // [ 修复 ] 4: 使用安全的 saveCOOToString (见下方)
        return saveCOOToString(result)
      }
    }
    return ""
  }

  // [ 修复 ] 修复了 isS 中的整型除法问题
  def isS (matrix: RDD[String], size: Int): Boolean = {
    // .sum() 返回 Double
    val numOF_NON_zero = matrix.flatMap{
      line =>
        val element = line.split(",").map(_.toDouble)
        element.map{
          vale =>
            if (vale != 0) 1.0 else 0.0 // 使用 Double
        }
    }.sum()

    if (size == 0) return false // 避免除以零

    val rate = numOF_NON_zero / size.toDouble
    println(s"Sparsity rate (non-zero): $rate")
    // 如果非零元素的比例低于 50%，则判断为稀疏
    if (rate < 0.5){
      true
    }
    else {
      false
    }
  }

  def printV (Result: RDD[Double]) = {
    // .collect() 对于小型的 *结果* 向量是允许的
    val list = Result.collect()
    println("Result Vector:")
    println(s"[${list.mkString(",")}]")
  }

  def printM (Result: RDD[(Int, Int, Double)]) = {
    // .collect() 对于小型的 *测试* 矩阵结果是允许的
    val list = Result.collect()
    println("Result Matrix in COO:")
    list.foreach(println)
  }

  // [ 修复 ] 替换了危险的 saveCOOAsMatrix
  // 这个函数只返回 COO 列表的字符串表示。
  // 它 *不会* 创建一个稠密的 Array，从而避免了 OOM。
  // 这仍然使用了 .collect()，对于一个巨大的结果矩阵来说是危险的。
  // 在 "生产" 代码中, 您应该使用 result.map(...).saveAsTextFile(...)
  def saveCOOToString(cooRDD: RDD[(Int, Int, Double)]): String = {
    // 警告：对于一个 *巨大* 的 *结果* 矩阵，.collect() 仍然会违反规定。
    // 假设在这里用于测试/演示的矩阵结果很小。
    val cooData = cooRDD.collect()
    // 将 (i, j, v) 元组转换为字符串，每行一个
    val matrixString = cooData.map { case (i, j, value) =>
      s"($i, $j, $value)"
    }.mkString("\n")
    matrixString
  }
}

//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.rdd.RDD
//import java.io._
//
//class Runner {
//  val Cal = new Calculator
//  val Con = new Converter
//  private val conf: SparkConf = new SparkConf()
//    .setAppName("CW1") // Set your application's name
//    .setMaster("local[*]") // Use all cores of the local machine
//    .set("spark.ui.enabled", "false")
//  val sc: SparkContext = new SparkContext(conf)
//  val savelink = "result/result.text"
//
//  def Run (address1: String, address2: String): String = {
//    //address1是稀疏矩阵的路径，因此不会进行判断，只判断了address2的
//    val SM = sc.textFile(address1)
//    val file = sc.textFile(address2)
//    val numOFrow = file.count().toInt
//    val numOFcol = file.first().split(",").length
//    val size = numOFrow * numOFcol
//
//    if (numOFrow == 1){
//      if (isS(file, size)){
//        val (rowOffset, colIndices, values, shape) = Con.SMToCSR(SM)(sc)
//        val (svIndices, svValues, vecLength) = Con.ReadSV(file)(sc)
//        val result = Cal.SpM_SpSV(rowOffset, colIndices, values, svIndices, svValues, shape, vecLength)(sc)
//        val resultString: String = result.map(_.toString).reduce(_ + "," + _)
//        printV(result)
//        println("SpM_SpSV")
//        return resultString
//      }
//      else {
//        val (rowOffset, colIndices, values, shape) = Con.SMToCSR(SM)(sc)
//        val (vector, n) = Con.ReadDV(file)(sc)
//        val result = Cal.SpM_DV(rowOffset, colIndices, values, vector, shape)(sc)
//        val resultString: String = result.map(_.toString).reduce(_ + "," + _)
//        printV(result)
//        println("SpM_DV")
//        return resultString
//      }
//    }
//    if (numOFrow > 1){
//      if (isS(file, size)){
//        val (rowOffset, colIndices, values, shape) = Con.SMToCSR(SM)(sc)
//        val (row, colOffset, value, shape2) = Con.SMToCSC(file)(sc)
//        val result = Cal.SpM_SpM(rowOffset, colIndices, values, colOffset, row, value,shape, shape2)(sc)
//        val resultShape = (shape._1, shape2._2)
//        printM(result)
//        println("SpM_SpM")
//        return saveCOOAsMatrix(result,resultShape)
//      }
//      else {
//        val (rowOffset, colIndices, values, shape) = Con.SMToCSR(SM)(sc)
//        val (matrix, shape2) = Con.ReadDM(file)(sc)
//        val result = Cal.SpM_SpDM(rowOffset, colIndices, values, matrix, shape, shape2)(sc)
//        val resultShape = (shape._1, shape2._2)
//        printM(result)
//        println("SpM_SpDM")
//        return saveCOOAsMatrix(result,resultShape)
//      }
//    }
//    return ""
//  }
//
//  def isS (matrix: RDD[String], size: Int): Boolean = {
//    val indexMatrix = matrix.zipWithIndex().map{
//      case (line, rowindex) => (line, rowindex.toInt)
//    }
//    val numOFzero = matrix.flatMap{
//      line =>
//        val element = line.split(",").map(_.toDouble)
//        element.map{
//          vale =>
//            if (vale != 0) 1 else 0
//        }
//    }.sum()
//    val rate = numOFzero/size
//    println(rate)
//    if (rate < 0.5){
//      true
//    }
//    else {
//      false
//    }
//  }
//  def printV (Result: RDD[Double]) = {
//    val num = Result.count().toInt
//    val List = Result.take(num).toList
//    println("Result Vector:")
//    List.foreach(println)
//  }
//  def printM (Result: RDD[(Int, Int, Double)]) = {
//    val num = Result.count().toInt
//    val List = Result.take(num).toList
//    println("Result Matrix in COO:")
//    List.foreach(println)
//  }
//  def saveCOOAsMatrix(cooRDD: RDD[(Int, Int, Double)], shape: (Int, Int)): String = {
//
//    // 1. 创建全零矩阵
//    val matrix = Array.ofDim[String](shape._1, shape._2)
//    for (i <- 0 until shape._1; j <- 0 until shape._2) {
//      matrix(i)(j) = "0"
//    }
//
//    // 2. 收集COO数据并填充矩阵
//    val cooData = cooRDD.collect()
//    cooData.foreach { case (i, j, value) =>
//      if (i < shape._1 && j < shape._2) {
//        matrix(i)(j) = f"$value%.1f"
//      }
//    }
//
//    // 3. 转换为字符串并保存
//    val matrixString = matrix.map(_.mkString(", ")).mkString("\n")
//    matrixString
//  }
//}
