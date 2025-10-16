package RCDPjour.Pre

import org.apache.spark.SparkContext
import scala.util.Random

object MoveSimExtractor {

  def ExtractDailyCommute(sc: SparkContext,
                          ODDataPath: String,
                          dispreoutputPath: String,
                          realoutputPath: String,
                          testoutputPath: String,
                          valoutputPath: String
                         ): Unit = {

    // 1. 读取原始数据
    val rawData = sc.textFile(ODDataPath)

    // 2. 添加随机索引并打乱数据
    val shuffledData = rawData.map(line => (Random.nextDouble(), line))
      .sortByKey()
      .map(_._2)
      .zipWithIndex()
      .cache()  // 缓存打乱后的数据

    // 3. 划分数据集
    // 3.1 划分dispre数据集 (前70000条)
    val dispreData = shuffledData.filter { case (_, index) => index < 500000 }
      .map { case (line, _) => line }

//    // 3.2 划分real数据集 (接下来的70000条)
//    val realData = shuffledData.filter { case (_, index) =>
//        index >= 70000 && index < 140000 }
//      .map { case (line, _) => line }
//
//    // 3.3 划分test数据集 (接下来的20000条)
//    val testData = shuffledData.filter { case (_, index) =>
//        index >= 140000 && index < 160000 }
//      .map { case (line, _) => line }
//
//    // 3.4 划分val数据集 (剩余的10000条)
//    val valData = shuffledData.filter { case (_, index) => index >= 160000 }
//      .map { case (line, _) => line }

    // 4. 保存结果
    dispreData.coalesce(1).saveAsTextFile(dispreoutputPath)
//    realData.coalesce(1).saveAsTextFile(realoutputPath)
//    testData.coalesce(1).saveAsTextFile(testoutputPath)
//    valData.coalesce(1).saveAsTextFile(valoutputPath)

    // 5. 释放缓存
    shuffledData.unpersist()
  }
}