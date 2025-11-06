/*
SMToCOO (address: String): (RDD[Int], RDD[Int], RDD[Double])
SMToCSC (address: String): (RDD[Int], RDD[Int], RDD[Double])
SMToCSR (address: String): (RDD[Int], RDD[Int], RDD[Double])
这三个方法包含了读取CSV文件并转换成COO，CSC，CSR这三种保存形式。结构会被保存在三个PDD中返回。

def SMToSELL (address: String, sliceHigh: Int): (RDD[Int], RDD[Int], RDD[Double])
这个方法是转换成SELL格式的方法，但是这个版本保存出来的结果并不正确，现在会按照row的先后顺序保存数据，但实际上应该按照col的顺序

所有格式转换的方法已经被封装进Converter类中，使用时需要先创建Converter实例再调用方法。

 */

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.io.Source

class Converter02 {
  private val conf: SparkConf = new SparkConf()
    .setAppName("CW1") // Set your application's name
    .setMaster("local[*]") // Use all cores of the local machine
    .set("spark.ui.enabled", "false")
  val sc: SparkContext = new SparkContext(conf)

  def SMToCOO (address: String): (RDD[Int], RDD[Int], RDD[Double], (Int, Int)) = {
    /*
    Row:       ,List(0, 0, 1, 3)
    Col:       ,List(0, 2, 1, 3)
    Value:     ,List(4.0, 9.0, 7.0, 5.0)
     */
    val matrix = sc.textFile(address)
    val numOFrow = matrix.count().toInt
    val numOFcol = matrix.first().split(",").length
    val indexMatrix = matrix.zipWithIndex().map{
      case (line, rowindex) => (line, rowindex.toInt)
    }
    val RDDvalue = indexMatrix.flatMap{
      case (line,rowindex) =>
        val ele = line.split(",").map(_.toDouble)
        ele.zipWithIndex.flatMap { case (value, colIndex) =>
          if (value != 0.0) Some((rowindex, colIndex, value))
          else None
        }
    }
    val rowIndex = RDDvalue.map(_._1)
    val colIndex = RDDvalue.map(_._2)
    val values = RDDvalue.map(_._3)

    (rowIndex, colIndex, values, (numOFrow, numOFcol))
    /*
        What have been done

        1.  `sc.textFile(address)`: 以分布式方式逐行读取CSV文件 (Transformation)。
        2.  `matrix.count()` / `matrix.first()`: 执行了两个 Spark Actions (行动) 来获取矩阵的维度（行数和列数）。
        3.  `matrix.zipWithIndex()`: 再次遍历文件，为每一行（row）附加一个唯一的行号 (Transformation)。
        4.  `indexMatrix.flatMap { ... }`: 这是核心。它遍历每一行，将该行拆分为列，过滤掉0值，并创建一个 *单一的、结构完美的 RDD*，名为 `RDDvalue`。
            - `RDDvalue` 的类型是 `RDD[(Int, Int, Double)]`，即 `RDD[(row, col, value)]`。
        5.  `val rowIndex = RDDvalue.map(_._1)`: 【问题点】创建了一个 *新* 的 RDD，只包含行索引。
        6.  `val colIndex = RDDvalue.map(_._2)`: 【问题点】创建了 *另一个* 新的 RDD，只包含列索引。
        7.  `val values = RDDvalue.map(_._3)`: 【问题点】创建了 *第三个* 新的 RDD，只包含值。
        8.  `return (rowIndex, colIndex, values, ...)`: 返回了这三个被“撕碎”的独立 RDD。

        二、 为什么这是一个严重问题？ (原因)

        我们返回了三个独立的 RDD。在 Spark 的分布式世界中，`rowIndex` 的第100个元素与 `colIndex` 的第100个元素**没有任何关联**，它们很可能存储在集群中完全不同的机器上。

        这导致了一个灾难性的后果：
        任何需要 *同时* 知道 (row, col, value) 信息的函数（比如我们的 `Calculator.scala` 或 `SMToCSC`），都必须首先“重建”这个结构。

        重建这个结构的唯一方法是：
        1.  `rowIndex.zipWithIndex()` -> `RDD[(Long, Int)]` (页码, row)
        2.  `colIndex.zipWithIndex()` -> `RDD[(Long, Int)]` (页码, col)
        3.  `values.zipWithIndex()`   -> `RDD[(Long, Double)]` (页码, value)
        4.  然后执行 `join` 和 `join` (例如：`indexedRow.join(indexedCol).join(indexedValue)`)。

        `join` 是 Spark 中最昂贵的操作之一，因为它会触发**大规模的网络 Shuffle (混洗)**。

        **结论：** 我们的 `Calculator.scala` 和 `SMToCSC` 之所以如此复杂和缓慢，是因为它们 90% 的工作都花在了执行昂贵的 `join` (Shuffle) 上，而这些 `join` 的唯一目的，就是为了**撤销**这个 `SMToCOO` 函数最后三行代码所做的“撕碎”操作。


        三、 应该如何修改？ (建议的解决方案)

        这个修改非常简单，但它会使我们所有其他的代码（Calculator, SMToCSC）都变得极其简单和高效。

        我们应该在 `RDDvalue`（即 `RDD[(Int, Int, Double)]`）那里停下，因为它已经是我们想要的完美形态了。

        1.  【删除】最后三行：
            // val rowIndex = RDDvalue.map(_._1) // <-- 删除
            // val colIndex = RDDvalue.map(_._2) // <-- 删除
            // val values = RDDvalue.map(_._3)   // <-- 删除

        2.  【修改】返回语句：
            // (rowIndex, colIndex, values, (numOFrow, numOFcol)) // <-- 替换为:
            (RDDvalue, (numOFrow, numOFcol))

        3.  【修改】函数签名（返回类型）：
            // def SMToCOO(...): (RDD[Int], RDD[Int], RDD[Double], (Int, Int)) // <-- 替换为:
            def SMToCOO(...): (RDD[(Int, Int, Double)], (Int, Int))

        **好处：**
        `Calculator` 现在会收到一个单一的 `RDD[(Int, Int, Double)]`。当它 `map` 这个 RDD 时，它在*一个元素*中就同时拥有了 `row`, `col`, 和 `value`。它不再需要任何 `zipWithIndex` 或 `join` 来重建数据，可以**立即开始计算**。

        */

    /*
  val RDDvalue: RDD[(Int, Int, Double)] = indexMatrix.flatMap {
      case (line,rowindex) =>
        val ele = line.split(",").map(_.toDouble)
        ele.zipWithIndex.flatMap { case (value, colIndex) =>
          // 过滤掉0值
          if (value != 0.0) Some((rowindex, colIndex, value))
          else None
        }
    }
    (RDDvalue, (numOFrow, numOFcol))
    * */
  }

  def SMToCSC (address: String): (RDD[Int], RDD[Int], RDD[Double], (Int, Int)) = {
    /*
    Row:       ,List(0, 1, 0, 3)
    ColOffset: ,List(0, 1, 2, 3, 4, 4)
    Value:     ,List(4, 7, 9, 5)
     */
    val (row, col, value, size) = SMToCOO(address)
    val indexedRow = row.zipWithIndex().map{
      case (r, i) => (i, r)
    }
    val indexedcol = col.zipWithIndex().map{
      case (c, i) => (i, c)
    }
    val indexedValue = value.zipWithIndex().map{
      case (v, i) => (i, v)
    }
    val RowCol = indexedRow.join(indexedcol)
    val RowColValue = RowCol.join(indexedValue)
    val cooRDD = RowColValue.map{
      case (_,((r, c), v)) => (r, c, v)
    }
    val sortedRDD = cooRDD.sortBy{
      case (r, c, v) => (c, r)
    }

    val rowIndexRDD = sortedRDD.map(_._1)
    val valueRDD = sortedRDD.map(_._3)
    val CList = col.take(col.count().toInt).toList
    val countMap = CList
      .groupBy(identity)
      .map { case (key, value) => (key, value.size) }

    val numList: List[Int] = (0 until size._2).map { index =>
      countMap.getOrElse(index, 0)
    }.toList

    val resultList = (0 :: numList).foldLeft((List.empty[Int], 0)){
      case ((offset, sum), current) =>
        val newsum = sum + current
        (offset :+ newsum, newsum)
    }._1

    val colOffset = sc.parallelize(resultList)
    (rowIndexRDD, colOffset, valueRDD, size)

    /*
    --- 【组员分析与重构注释】 ---

    一、 这个函数做了什么？

    1.  `SMToCOO(address)`: 调用 `SMToCOO`，接收三个被撕碎的RDD (`row`, `col`, `value`)。
    2.  `zipWithIndex()` (x3): 为这三个独立的RDD分别创建索引。
    3.  `indexedRow.join(indexedcol)`: 执行**第一次昂贵的 Shuffle**，将 (页码, row) 和 (页码, col) 合并。
    4.  `RowCol.join(indexedValue)`: 执行**第二次昂贵的 Shuffle**，将 (页码, (row, col)) 和 (页码, value) 合并。
    5.  `cooRDD = ...`: 经过两次昂贵的 `join`，终于重建了我们本应该从 `SMToCOO` 直接返回的 `RDD[(Int, Int, Double)]`。
    6.  `cooRDD.sortBy(...)`: 执行**第三次昂贵的 Shuffle** (`sortBy` 是一种代价极高的全集群排序)。
    7.  `rowIndexRDD = sortedRDD.map(_._1)`: 从排序后的RDD中提取 `row` RDD。
    8.  `valueRDD = sortedRDD.map(_._3)`: 从排序后的RDD中提取 `value` RDD。
    9.  `CList = col.take(col.count().toInt).toList`: **【严重错误 1】**。`col.count()` 是一个 Action (作业1)。`col.take(...)` 是第二个 Action (作业2)。`.toList` 将*所有*的列索引数据（可能有10亿个）从集群拉取到**Driver (本地) 内存**中。
    10. `countMap = CList.groupBy(...)`: **【严重错误 2】**。`CList` 是一个*本地 List*。所有 `groupBy`, `map`, `foldLeft` 操作都是在**Driver (本地) 机器**上执行的，而不是在 Spark 集群上。
    11. `colOffset = sc.parallelize(resultList)`: 将在 Driver 上计算出的本地 `resultList` 重新广播回集群，创建 `colOffset` RDD。

    二、 存在的问题

    1.  **扩展性彻底失败 (Fatal)：** `col.take(...).toList` 会在处理任何真实世界的数据集时导致 **Driver OutOfMemoryError**。这个函数不是一个真正的分布式程序。
    2.  **效率极其低下：** 它执行了三次昂贵的 Shuffle（`join`, `join`, `sortBy`），而前两次 `join` 只是为了修复 `SMToCOO` 的设计缺陷。
    3.  **逻辑错误：** `colOffset` 是根据 *原始的、未排序的* `col` RDD 计算的。而 `rowIndexRDD` 和 `valueRDD` 是根据 `sortedRDD`（按列排序）计算的。这两个数据集的顺序是不匹配的！返回的 `colOffset` 和 `(rowIndexRDD, valueRDD)` 组合在一起是无效的CSC数据。

    三、 应该如何修改？

    1.  **前提：** 假设 `SMToCOO` 已经被修正，返回单一的 `cooRDD: RDD[(Int, Int, Double)]`。
    2.  **删除**所有的 `zipWithIndex` 和 `join`。
    3.  **删除**所有的 `.toList` 和本地计算。
    4.  **职责分离：** `Converter` (加载器) 不应该转换格式。`Engine` (计算器) 应该在其内部按需进行转换。
    5.  **建议：** 从 `Converter` 中删除此方法。
    6.  **(如果必须实现)：** *正确*的**分布式**方法是：
        ```scala
        // 1. 假设 cooRDD: RDD[(Int, Int, Double)]
        // 2. 映射为 (col, (row, value))
        val byCol = cooRDD.map { case (i, j, v) => (j, (i, v)) }

        // 3. 按列ID分组 (这会触发一次 Shuffle，这是必要的代价)
        // 这就是“分布式 CSC”
        val distributedCSC: RDD[(Int, Iterable[(Int, Double)])] = byCol.groupByKey()

        // 4. (如果必须得到传统格式)
        // 在分布式 RDD 上计算偏移量
        val colCounts = byCol.countByKey() // Action, 但只返回一个小的 Map[Int, Long]
        // ... 然后在 Driver 上安全地处理这个 *小* Map 来创建 colOffset RDD

        // 5. 对 `distributedCSC` 按key排序（如果需要），然后 flatMap 以获取 row/value RDDs
        // ...
        ```
    */
  }

  def SMToCSR (address: String): (RDD[Int], RDD[Int], RDD[Double], (Int, Int)) = {
    /*
    RowOffset: ,List(0, 2, 3, 3, 4)
    Col:       ,List(0, 2, 1, 3)
    Value:     ,List(4.0, 9.0, 7.0, 5.0)
     */
    val (row, col, value, size) = SMToCOO(address)
    val RList = row.take(size._1).toList
    val countMap = RList
      .groupBy(identity)
      .map { case (key, value) => (key, value.size) }

    val numList: List[Int] = (0 until size._1).map { index =>
      countMap.getOrElse(index, 0)
    }.toList

    val resultList = (0 :: numList).foldLeft((List.empty[Int], 0)){
      case ((offset, sum), current) =>
        val newsum = sum + current
        (offset :+ newsum, newsum)
    }._1

    val rowOffset = sc.parallelize(resultList)
    (rowOffset, col, value, size)

    /*
    --- 【组员分析与重构注释】 ---

    一、 这个函数做了什么？

    1.  `SMToCOO(address)`: 调用 `SMToCOO`，接收三个被撕碎的RDD (`row`, `col`, `value`)。
    2.  `RList = row.take(size._1).toList`: **【严重错误 1】**。与 `SMToCSC` 完全相同的问题。它将*所有*的行索引（可能10亿个）拉到 **Driver (本地) 内存**中。
    3.  `countMap = RList.groupBy(...)`: **【严重错误 2】**。所有计算（`groupBy`, `map`, `foldLeft`）都发生在**Driver (本地) 机器**上。
    4.  `rowOffset = sc.parallelize(resultList)`: 将本地计算的结果重新广播回集群。
    5.  `return (rowOffset, col, value, size)`: 返回新计算的 `rowOffset` RDD，以及*原始的、未排序的* `col` 和 `value` RDD。

    二、 存在的问题

    1.  **扩展性彻底失败 (Fatal)：** `row.take(...).toList` 会在处理大文件时**100%导致Driver OutOfMemoryError**。这不是一个分布式程序。
    2.  **逻辑错误 (Fatal)：** `rowOffset` RDD 是 CSR 格式的“行指针”。CSR 格式**严格**要求 `col` 和 `value` RDD 必须是**按行（row-major）排序**的。而您返回的 `col` 和 `value` RDD 是从 `SMToCOO` 直接传过来的，它们是*未排序*的（或者说是按 `flatMap` 的顺序）。
    3.  **结论：** 这个函数返回的数据是**无效的、损坏的、不可用的**。`Calculator` 如果尝试使用它们，会得到完全错误的答案。

    三、 应该如何修改？

    1.  **前提：** 假设 `SMToCOO` 已经被修正，返回单一的 `cooRDD: RDD[(Int, Int, Double)]`。
    2.  **职责分离：** 此函数应从 `Converter` (加载器) 中删除。
    3.  **正确实现 (在 `Engine` 类内部)：** SpMV (矩阵-向量乘法) 是唯一需要 CSR 的操作。最高效的方法是创建一个“分布式 CSR” 并将其*缓存*起来。
        ```scala
        // 在 Engine/Calculator class 内部：

        // 假设 cooRDD: RDD[(Int, Int, Double)] 是类的成员

        // 使用 lazy val，这个昂贵的转换只会在第一次调用 SpMV 时执行一次
        lazy val distributedCSR: RDD[(Int, Iterable[(Int, Double)])] = {
          cooRDD.map { case (i, j, v) => (i, (j, v)) } // (row, (col, val))
                .groupByKey() // <-- 分布式 Shuffle，安全且可扩展
                .persist(StorageLevel.MEMORY_AND_DISK) // 缓存结果供将来使用
        }

        // 然后 spmv 方法就可以直接使用这个 `distributedCSR` RDD，
        // 实现零-Shuffle 的最高性能。
        ```
    */
  }


  def ReadSV (address: String): (RDD[Int], RDD[Double], Int) = {
    /*
    Idices:    ,List(1, 4, 5, 9)
    values:    ,List(1, 4, 7, 4)
    */
    val vector = sc.textFile(address)
    val numOFrow = vector.count()
    val numOFcol = vector.first().split(",").length
    if (numOFrow != 1){
      println("The input is not a vector.")
      return (sc.parallelize(List.empty[Int]), sc.parallelize(List.empty[Double]), 0)
    }
    val vectorArr = vector.map{
      values =>
        val ele = values.split(",").map(_.toDouble)
        val nonele = for {
          index <- ele.indices
          value = ele(index)
          if value != 0.0
        } yield (index, value)
        nonele
    }.flatMap(identity)
    ((vectorArr.map(_._1)), (vectorArr.map(_._2)), numOFcol)

    /*
    --- 【组员分析与重构注释】 ---

    一、 这个函数做了什么？

    1.  `sc.textFile(address)`: 分布式地读取文件（即使它只有一行）。
    2.  `vector.map{ ... }.flatMap(identity)`: 这一步做得很好。它将 `0,5,0,2` 这样的行转换为 `(index, value)` 对，并过滤掉0值。
    3.  `vectorArr` 是我们想要的 `RDD[(Int, Double)]` (即 `RDD[(index, value)]`)。
    4.  `return ((vectorArr.map(_._1)), (vectorArr.map(_._2)), ...)`: **【问题点】** 和 `SMToCOO` 一样，它在最后一步把这个完美的“单一RDD”**撕碎**成了两个独立的 RDD (`RDD[Int]` 和 `RDD[Double]`)。

    二、 存在的问题

    1.  **“破碎的”接口：** `Calculator` 在接收到这两个 RDD 后，必须执行 `zip` 或 `zipWithIndex().join()` 来将它们重新组合，这又是一次不必要的 **Shuffle**。

    三、 应该如何修改？

    1.  【删除】最后一行中的 `map`：
        // ((vectorArr.map(_._1)), (vectorArr.map(_._2)), numOFcol) // <-- 替换为:
        (vectorArr, numOFcol)
    2.  【修改】函数签名（返回类型）为：
        // def ReadSV(...): (RDD[Int], RDD[Double], Int) // <-- 替换为:
        def ReadSV(...): (RDD[(Int, Double)], Int)
    */
  }

  def ReadDV (address: String): (RDD[Double], Int) = {
    val vector = sc.textFile(address)
    val numOFrow = vector.count()
    if (numOFrow != 1){
      println("The input is not a vector.")
      return (sc.parallelize(List.empty[Double]), 0)
    }
    val vectorArr = vector.first().split(",").map(_.toDouble)
    val vectorRDD = sc.parallelize(vectorArr)
    val numOFcol = vectorArr.length
    (vectorRDD, numOFcol)

    /*
    --- 【组员分析与重构注释】 ---

    一、 这个函数做了什么？

    1.  `vector.first()`: **【严重错误 1】**。这是一个 **Action**，它将文件的第一行（即整个向量）拉到 **Driver (本地) 内存**中。
    2.  `vectorArr = ... .split(...)`: **【严重错误 2】**。所有的数据处理（`split`, `map`）都发生在 **Driver (本地)**。
    3.  `sc.parallelize(vectorArr)`: 将 Driver 上的本地 `Array` 重新广播回集群。
    4.  `return (vectorRDD, ...)`: 返回的 `RDD[Double]` **丢失了它的索引信息**。

    二、 存在的问题

    1.  **扩展性为零 (Fatal)：** 这是一个单机程序，不是 Spark 程序。如果向量有10亿个元素（例如一个稠密的用户特征向量），它会导致 **Driver OutOfMemoryError**。
    2.  **信息丢失：** `Calculator` 在收到 `RDD[Double]` 后，不知道哪个值对应哪个索引。它必须执行 `zipWithIndex()`（一次昂贵的 **Shuffle**）来重新创建索引 `(index, value)`，以便与矩阵的 `col` 索引进行 `join`。

    三、 应该如何修改？

    1.  **不要使用 `.first()`！**
    2.  使用 `ReadSV` 中正确的*分布式*读取逻辑（`vector.map{...}.flatMap(identity)`），但**去掉** `if (value != 0.0)` 的过滤。
    3.  返回**单一的** `RDD[(Int, Double)]`。

    ```scala
    // --- 建议的 ReadDV 实现 ---
    def ReadDV_improved(address: String): (RDD[(Int, Double)], Int) = {
        val vector = sc.textFile(address)
        val numOFcol = vector.first().split(",").length // (这行仍然需要在Driver上，但没办法)

        val vectorRDD = vector.flatMap { line =>
            line.split(",").map(_.toDouble).zipWithIndex
        }
        .map { case (value, index) => (index.toInt, value) } // (index, value)

        (vectorRDD, numOFcol)
    }
    ```
    */
  }

  def ReadDM (address: String): (RDD[Array[Double]], (Int, Int)) = {
    val matrix = sc.textFile(address)
    val numOFrow = matrix.count().toInt
    if (numOFrow == 0) {
      println("The input matrix is empty.")
      return (sc.parallelize(Seq.empty[Array[Double]]),(0, 0))
    }
    val RDDMatrix = matrix.map{
      line =>
        line.split(",").map(_.toDouble)
    }
    val numOFcol = RDDMatrix.first().length
    (RDDMatrix, (numOFrow,numOFcol))

    /*
    --- 【组员分析与重构注释】 ---

    一、 这个函数做了什么？

    1.  `sc.textFile(address)`: 分布式读取文件。
    2.  `matrix.map { line => ... }`: 以分布式方式将每一行 `String` 转换为 `Array[Double]`。
    3.  `return (RDDMatrix, ...)`: 返回一个**单一的、结构完整的 RDD**，即 `RDD[Array[Double]]`。

    二、 存在的问题

    * **几乎没有问题！**
    * 这是一个**优秀**的加载函数。
    * 它**是分布式的**（没有 `.toList` 或 `.first()` 的数据处理）。
    * 它**返回一个单一的 RDD**（没有“撕碎”数据）。

    三、 应该如何修改？

    * **无需修改。**
    * **这是您所有其他加载函数（`SMToCOO`, `ReadSV`, `ReadDV`）都应该模仿的“好模式”。**
    * （唯一的微小问题是 `matrix.count()` 和 `matrix.first()` 会触发两次 Action，但这通常是获取维度的必要代价。）
    */
  }
}

