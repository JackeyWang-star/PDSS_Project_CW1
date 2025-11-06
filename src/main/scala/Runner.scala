import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

class Runner {
  val Cal = new Calculator
  val Con = new Converter
  private val conf: SparkConf = new SparkConf()
    .setAppName("CW1") // Set your application's name
    .setMaster("local[*]") // Use all cores of the local machine
    .set("spark.ui.enabled", "false")
  val sc: SparkContext = new SparkContext(conf)

  def Run (address1: String, address2: String) = {
    //address1是稀疏矩阵的路径，因此不会进行判断，只判断了address2的
    val SM = sc.textFile(address1)
    val file = sc.textFile(address2)
    val numOFrow = file.count().toInt
    val numOFcol = file.first().split(",").length
    val size = numOFrow * numOFcol

    if (numOFrow == 1){
      if (isS(file, size)){
        val (rowOffset, colIndices, values, shape) = Con.SMToCSR(SM)(sc)
        val (svIndices, svValues, vecLength) = Con.ReadSV(file)(sc)
        val result = Cal.SpM_SpSV(rowOffset, colIndices, values, svIndices, svValues, shape, vecLength)(sc)
        printV(result)
        println("SpM_SpSV")
      }
      else {
        val (rowOffset, colIndices, values, shape) = Con.SMToCSR(SM)(sc)
        val (vector, n) = Con.ReadDV(file)(sc)
        val result = Cal.SpM_DV(rowOffset, colIndices, values, vector, shape)(sc)
        printV(result)
        println("SpM_DV")
      }
    }
    if (numOFrow > 1){
      if (isS(file, size)){
        val (rowOffset, colIndices, values, shape) = Con.SMToCSR(SM)(sc)
        val (row, colOffset, value, shape2) = Con.SMToCSC(file)(sc)
        val result = Cal.SpM_SpM(rowOffset, colIndices, values, colOffset, row, value,shape, shape2)(sc)
        printM(result)
        println("SpM_SpM")
      }
      else {
        val (rowOffset, colIndices, values, shape) = Con.SMToCSR(SM)(sc)
        val (matrix, shape2) = Con.ReadDM(file)(sc)
        val result = Cal.SpM_SpDM(rowOffset, colIndices, values, matrix, shape, shape2)(sc)
        printM(result)
        println("SpM_SpDM")
      }
    }
  }

  def isS (matrix: RDD[String], size: Int): Boolean = {
    val indexMatrix = matrix.zipWithIndex().map{
      case (line, rowindex) => (line, rowindex.toInt)
    }
    val numOFzero = matrix.flatMap{
      line =>
        val element = line.split(",").map(_.toDouble)
        element.map{
          vale =>
            if (vale != 0) 1 else 0
        }
    }.sum()
    val rate = numOFzero/size
    println(rate)
    if (rate < 0.5){
      true
    }
    else {
      false
    }
  }
  def printV (Result: RDD[Double]) = {
    val num = Result.count().toInt
    val List = Result.take(num).toList
    println("Result Vector:")
    List.foreach(println)
  }
  def printM (Result: RDD[(Int, Int, Double)]) = {
    val num = Result.count().toInt
    val List = Result.take(num).toList
    println("Result Matrix in COO:")
    List.foreach(println)
  }
}
