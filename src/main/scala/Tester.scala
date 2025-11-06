/*
运行PDD时会输出很多红色日志，目前我还没找到把它关闭的方法。
运行前需要注意JAVA CDK的版本，最好使用JAVA8或者JAVA11
*/

object Tester {
  def main(args: Array[String]): Unit = {
    // Set log level
    org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.ERROR)
    org.apache.log4j.Logger.getLogger("akka").setLevel(org.apache.log4j.Level.ERROR)

    val Con = new Converter
    val Cal = new Calculator
    val Run = new Runner
    val sc = Run.sc

    //isS test
//    val file = sc.textFile("Data/SMConverter_test.csv")
//    val numOFrow = file.count().toInt
//    val numOFcol = file.first().split(",").length
//    val size = numOFrow * numOFcol
//    val a = Run.isS(file, size)
//    sc.stop()
//    println(a)
    //SpM_DV test
//    Run.Run("Data/SMConverter_test.csv", "Data/DVConverter_test.csv")
    //SpM_SpSV test
//    Run.Run("Data/SMConverter_test.csv", "Data/SVConverter_test.csv")
    //SpM_SpM test
//    Run.Run("Data/SMConverter_test.csv", "Data/SMSM_test.csv")
    //SpM_SpDM test
    Run.Run("Data/SMConverter_test.csv", "Data/SMDM_test.csv")
  }
}
