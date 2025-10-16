//package RCDPjour.evaluation
//
//import org.apache.spark.SparkContext
//import scala.math.{log, sqrt}  // 明确导入sqrt函数
//
//object MI {
//
//  def CalMI_hw(sc: SparkContext,
//             synDataPath: String,
//             commuteDataPath: String): Unit = {
//
//    // 从行数据中提取家区域和工作区域对
//    def extractRegionPair(record: String): (String, String) = {
//      val parts = record.split(",")
//      if (parts.length < 2) ("", "") else {
//        val homePart = parts(0).split(" ")
//        val workPart = parts(1).split(" ")
//        val homeRegion = if (homePart.length > 1) homePart(1) else ""
//        val workRegion = if (workPart.length > 1) workPart(1) else ""
//        (homeRegion, workRegion)
//      }
//    }
//
//    // 读取并处理synData数据
//    val synPairs = sc.textFile(synDataPath)
//      .map(extractRegionPair)
//      .filter { case (h, w) => h.nonEmpty && w.nonEmpty }
//      .zipWithIndex()  // 添加索引
//      .map { case (pair, index) => (index, pair) }  // 转为(index, pair)格式
//      .persist()
//
//    // 读取并处理commuteData数据
//    val comPairs = sc.textFile(commuteDataPath)
//      .map(extractRegionPair)
//      .filter { case (h, w) => h.nonEmpty && w.nonEmpty }
//      .zipWithIndex()  // 添加索引
//      .map { case (pair, index) => (index, pair) }  // 转为(index, pair)格式
//      .persist()
//
//    // 确保两个数据集大小相同
//    val synCount = synPairs.count()
//    val comCount = comPairs.count()
//    if (synCount != comCount) {
//      println(s"警告: 数据集大小不同 (synData: $synCount, commuteData: $comCount)，结果可能不准确")
//    }
//
//    // 合并数据集：hcom, wcom, hsyn, wsyn
//    val combined = comPairs.join(synPairs)
//      .filter { case (_, (comPair, synPair)) =>
//        comPair._1.nonEmpty && comPair._2.nonEmpty && synPair._1.nonEmpty && synPair._2.nonEmpty
//      }
//      .map { case (_, (comPair, synPair)) =>
//        (comPair, synPair)  // (hcom, wcom), (hsyn, wsyn)
//      }
//      .persist()
//
//    val total = combined.count().toDouble
//    println(s"有效合并记录数: $total")
//
//    if (total == 0) {
//      println("错误: 没有有效记录，无法计算互信息")
//      return
//    }
//
//    // 计算联合分布 P(X=x, Y=y)
//    val jointDist = combined.map { case (x, y) => ((x, y), 1.0 / total) }
//      .reduceByKey(_ + _)
//
//    // 计算X的边际分布 P(X=x)
//    val xMarginal = combined.map { case (x, _) => (x, 1.0 / total) }
//      .reduceByKey(_ + _)
//
//    // 计算Y的边际分布 P(Y=y)
//    val yMarginal = combined.map { case (_, y) => (y, 1.0 / total) }
//      .reduceByKey(_ + _)
//
//    // 广播边际分布以提高性能
//    val bcXMarginal = sc.broadcast(xMarginal.collectAsMap())
//    val bcYMarginal = sc.broadcast(yMarginal.collectAsMap())
//
//    // 计算互信息
//    val mutualInfo = jointDist.map { case ((x, y), p_xy) =>
//      val p_x = bcXMarginal.value.getOrElse(x, 0.0)
//      val p_y = bcYMarginal.value.getOrElse(y, 0.0)
//
//      if (p_xy > 0 && p_x > 0 && p_y > 0) {
//        p_xy * log(p_xy / (p_x * p_y))
//      } else {
//        0.0
//      }
//    }.sum()
//
//    // 归一化处理
//    // 计算X的熵
//    val entropyX = xMarginal.map { case (_, p) =>
//      if (p > 0) -p * log(p) else 0.0
//    }.sum()
//
//    // 计算Y的熵
//    val entropyY = yMarginal.map { case (_, p) =>
//      if (p > 0) -p * log(p) else 0.0
//    }.sum()
//
//    // 计算归一化互信息
//    val normalizedMI = if (entropyX > 0 && entropyY > 0) {
//      mutualInfo / sqrt(entropyX * entropyY)  // 使用导入的sqrt函数
//    } else {
//      0.0
//    }
//
//    // 打印结果到控制台
//    println("======================================")
//    println(s"家区域-工作区域对互信息计算结果")
//    println(f"互信息值: $mutualInfo")
//    println(f"X的熵: $entropyX")
//    println(f"Y的熵: $entropyY")
//    println(f"归一化互信息: $normalizedMI")
//    println("======================================")
//
//    // 释放缓存
//    synPairs.unpersist()
//    comPairs.unpersist()
//    combined.unpersist()
//  }
//}

package RCDPjour.evaluation

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.math.log

object MI {

  def CalMI(sc: SparkContext,
            synDataPath: String,
            commuteDataPath: String): Unit = {

    // 读取数据，每行作为字符串
    val synRDD: RDD[String] = sc.textFile(synDataPath).map(_.trim).filter(_.nonEmpty)
    val comRDD: RDD[String] = sc.textFile(commuteDataPath).map(_.trim).filter(_.nonEmpty)

    // 检查数据集行数是否一致
    val synCount = synRDD.count()
    val comCount = comRDD.count()
    require(synCount == comCount,
      s"Datasets must have the same number of rows. Found synData: $synCount, commuteData: $comCount")

    // 为两个RDD添加索引并配对 (index, string)
    val synWithIndex = synRDD.zipWithIndex().map(_.swap)  // (index, synString)
    val comWithIndex = comRDD.zipWithIndex().map(_.swap)  // (index, comString)

    // 通过索引join确保正确配对
    val pairedRDD: RDD[(String, String)] = synWithIndex.join(comWithIndex)
      .values  // 只保留(synString, comString)对

    // 计算总样本数
    val total: Double = pairedRDD.count()

    // 计算X的边缘概率分布
    val xProb: collection.Map[String, Double] =
      synRDD.map(x => (x, 1))
        .reduceByKey(_ + _)
        .mapValues(_ / total)
        .collectAsMap()

    // 计算Y的边缘概率分布
    val yProb: collection.Map[String, Double] =
      comRDD.map(y => (y, 1))
        .reduceByKey(_ + _)
        .mapValues(_ / total)
        .collectAsMap()

    // 计算联合概率分布
    val jointProb: collection.Map[(String, String), Double] =
      pairedRDD.map(pair => (pair, 1))
        .reduceByKey(_ + _)
        .mapValues(_ / total)
        .collectAsMap()

    // 计算边缘熵 H(X)
    val hX = xProb.values.map(p => if (p > 0.0) -p * log2(p) else 0.0).sum

    // 计算边缘熵 H(Y)
    val hY = yProb.values.map(p => if (p > 0.0) -p * log2(p) else 0.0).sum

    // 计算联合熵 H(X,Y)
    val hXY = jointProb.values.map(p => if (p > 0.0) -p * log2(p) else 0.0).sum

    // 计算互信息 I(X;Y) = H(X) + H(Y) - H(X,Y)
    val mi = hX + hY - hXY

    // 计算标准化互信息 (Normalized Mutual Information)
    val nmi = if (hX > 0 && hY > 0) mi / math.sqrt(hX * hY) else 0.0

    // 打印结果
    println("=" * 50)
    println(s"Mutual Information Calculation Results")
    println(s"Total samples: $total")
    println(s"H(X) = $hX")
    println(s"H(Y) = $hY")
    println(s"H(X,Y) = $hXY")
    println(s"Mutual Information I(X;Y) = $mi")
    println(s"Normalized Mutual Information (NMI) = $nmi")
    println("=" * 50)
  }

  // 计算以2为底的对数工具函数
  private def log2(x: Double): Double = log(x) / log(2)
}