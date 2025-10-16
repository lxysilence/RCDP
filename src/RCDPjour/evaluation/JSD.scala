package RCDPjour.evaluation

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.math._

object JSD {
  def AdaJSD_hw(sc: SparkContext,
                synDataPath: String,
                commuteDataPath: String): Unit = {

    // 提取家庭和工作区域对
    def extractRegionPair(record: String): (String, String) = {
      val parts = record.split(",")
      val homePart = parts(0)
      val workPart = parts(1)
      (homePart, workPart)
    }

    // 计算概率分布
    def calculateDistribution(rdd: RDD[(String, String)]): Map[(String, String), Double] = {
      val total = rdd.count().toDouble
      rdd.map(pair => (pair, 1))
        .reduceByKey(_ + _)
        .collect()
        .map { case (pair, count) => (pair, count / total) }
        .toMap
    }

    // 计算KL散度
    def klDivergence(p: Double, q: Double): Double = {
      if (p > 0 && q > 0) p * log(p / q)
      else 0.0
    }

    // 读取并处理synData数据
    val synPairs = sc.textFile(synDataPath)
      .map(extractRegionPair)
      .filter(pair => pair._1.nonEmpty && pair._2.nonEmpty)

    val synDist = calculateDistribution(synPairs)
    println(s"SynData区域对数量: ${synDist.size}")

    // 读取并处理commuteData数据
    val comPairs = sc.textFile(commuteDataPath)
      .map(extractRegionPair)
      .filter(pair => pair._1.nonEmpty && pair._2.nonEmpty)

    val comDist = calculateDistribution(comPairs)
    println(s"CommuteData区域对数量: ${comDist.size}")

    // 获取所有区域对
    val allPairs = (synDist.keySet ++ comDist.keySet).toSet
    println(s"总区域对数量: ${allPairs.size}")

    // 计算JSD散度
    val jsd = allPairs.foldLeft(0.0) { (sum, pair) =>
      val p = synDist.getOrElse(pair, 0.0)
      val q = comDist.getOrElse(pair, 0.0)
      val m = (p + q) / 2.0

      // 计算KL(P||M)和KL(Q||M)
      val klPM = klDivergence(p, m)
      val klQM = klDivergence(q, m)

      sum + klPM + klQM
    } / 2.0

    // 打印结果到控制台
    println("======================================")
    println(s"区域Jensen-Shannon散度计算结果")
    println(f"JSD值: $jsd")
    println("======================================")
  }








  def CalJSD_hw(sc: SparkContext,
                synDataPath: String,
                commuteDataPath: String): Unit = {

    // 提取家庭和工作区域对
    def extractRegionPair(record: String): (String, String) = {
      val parts = record.split(",")
      if (parts.length < 2) ("", "") else {
        val homePart = parts(0).split(" ")
        val workPart = parts(1).split(" ")
        val homeRegion = if (homePart.length > 1) homePart(1) else ""
        val workRegion = if (workPart.length > 1) workPart(1) else ""
        (homeRegion, workRegion)
      }
    }

    // 计算概率分布
    def calculateDistribution(rdd: RDD[(String, String)]): Map[(String, String), Double] = {
      val total = rdd.count().toDouble
      rdd.map(pair => (pair, 1))
        .reduceByKey(_ + _)
        .collect()
        .map { case (pair, count) => (pair, count / total) }
        .toMap
    }

    // 计算KL散度
    def klDivergence(p: Double, q: Double): Double = {
      if (p > 0 && q > 0) p * log(p / q)
      else 0.0
    }

    // 读取并处理synData数据
    val synPairs = sc.textFile(synDataPath)
      .map(extractRegionPair)
      .filter(pair => pair._1.nonEmpty && pair._2.nonEmpty)

    val synDist = calculateDistribution(synPairs)
    println(s"SynData区域对数量: ${synDist.size}")

    // 读取并处理commuteData数据
    val comPairs = sc.textFile(commuteDataPath)
      .map(extractRegionPair)
      .filter(pair => pair._1.nonEmpty && pair._2.nonEmpty)

    val comDist = calculateDistribution(comPairs)
    println(s"CommuteData区域对数量: ${comDist.size}")

    // 获取所有区域对
    val allPairs = (synDist.keySet ++ comDist.keySet).toSet
    println(s"总区域对数量: ${allPairs.size}")

    // 计算JSD散度
    val jsd = allPairs.foldLeft(0.0) { (sum, pair) =>
      val p = synDist.getOrElse(pair, 0.0)
      val q = comDist.getOrElse(pair, 0.0)
      val m = (p + q) / 2.0

      // 计算KL(P||M)和KL(Q||M)
      val klPM = klDivergence(p, m)
      val klQM = klDivergence(q, m)

      sum + klPM + klQM
    } / 2.0

    // 打印结果到控制台
    println("======================================")
    println(s"区域Jensen-Shannon散度计算结果")
    println(f"JSD值: $jsd")
    println("======================================")
  }

  def CalJSD_d(sc: SparkContext,
                synDataPath: String,
                commuteDataPath: String): Unit = {

    // 提取工作时长
    def extractRegionPair(record: String): (String) = {
      val parts = record.split(",")
      parts(2)
    }

    // 计算概率分布
    def calculateDistribution(rdd: RDD[String]): Map[String, Double] = {
      val total = rdd.count().toDouble
      rdd.map(pair => (pair, 1))
        .reduceByKey(_ + _)
        .collect()
        .map { case (pair, count) => (pair, count / total) }
        .toMap
    }

    // 计算KL散度
    def klDivergence(p: Double, q: Double): Double = {
      if (p > 0 && q > 0) p * log(p / q)
      else 0.0
    }

    // 读取并处理synData数据
    val synPairs = sc.textFile(synDataPath)
      .map(extractRegionPair)
      .filter(pair => pair.nonEmpty)

    val synDist = calculateDistribution(synPairs)
    println(s"SynData工作时长数量: ${synDist.size}")

    // 读取并处理commuteData数据
    val comPairs = sc.textFile(commuteDataPath)
      .map(extractRegionPair)
      .filter(pair => pair.nonEmpty)

    val comDist = calculateDistribution(comPairs)
    println(s"CommuteData工作时长数量: ${comDist.size}")

    // 获取所有区域对
    val allPairs = (synDist.keySet ++ comDist.keySet).toSet
    println(s"总工作时长数量: ${allPairs.size}")

    // 计算JSD散度
    val jsd = allPairs.foldLeft(0.0) { (sum, pair) =>
      val p = synDist.getOrElse(pair, 0.0)
      val q = comDist.getOrElse(pair, 0.0)
      val m = (p + q) / 2.0

      // 计算KL(P||M)和KL(Q||M)
      val klPM = klDivergence(p, m)
      val klQM = klDivergence(q, m)

      sum + klPM + klQM
    } / 2.0

    // 打印结果到控制台
    println("======================================")
    println(s"工作时长Jensen-Shannon散度计算结果")
    println(f"JSD值: $jsd")
    println("======================================")
  }

  def CalJSD_c(sc: SparkContext,
               synDataPath: String,
               commuteDataPath: String): Unit = {

    // 提取通勤时长信息
    def extractCommuteTime(record: String): Int = {
      val parts = record.split(",")
      if (parts.length < 4) 0 else {
        try {
          // 解析出发时间和到达时间
          val departTime = parts(0).split(" ")(0).toInt
          val arriveTime = parts(1).split(" ")(0).toInt
          val commuteDiff = parts(3).toInt

          // 计算通勤时长：(到达时间-出发时间)*2 + 通勤差异
          (arriveTime - departTime) * 2 + commuteDiff
        } catch {
          case _: Exception => 0
        }
      }
    }

    // 计算概率分布
    def calculateDistribution(rdd: RDD[Int]): Map[Int, Double] = {
      val total = rdd.count().toDouble
      if (total == 0) Map.empty[Int, Double] else {
        rdd.map(time => (time, 1.0 / total))
          .reduceByKey(_ + _)
          .collect()
          .toMap
      }
    }

    // 计算KL散度
    def klDivergence(p: Double, q: Double): Double = {
      if (p > 0 && q > 0) p * log(p / q)
      else 0.0
    }

    // 读取并处理synData数据
    val synTimes = sc.textFile(synDataPath)
      .map(extractCommuteTime)
      .filter(_ > 0)  // 过滤无效时长
      .persist()  // 缓存以提高性能

    val synDist = calculateDistribution(synTimes)
    println(s"SynData有效记录数: ${synTimes.count()}")
    println(s"SynData通勤时长类别数: ${synDist.size}")

    // 读取并处理commuteData数据
    val comTimes = sc.textFile(commuteDataPath)
      .map(extractCommuteTime)
      .filter(_ > 0)  // 过滤无效时长
      .persist()  // 缓存以提高性能

    val comDist = calculateDistribution(comTimes)
    println(s"CommuteData有效记录数: ${comTimes.count()}")
    println(s"CommuteData通勤时长类别数: ${comDist.size}")

    // 获取所有通勤时长值
    val allTimes = (synDist.keySet ++ comDist.keySet).toSet
    println(s"总通勤时长类别数: ${allTimes.size}")

    // 计算JSD散度
    val jsd = allTimes.foldLeft(0.0) { (sum, time) =>
      val p = synDist.getOrElse(time, 0.0)
      val q = comDist.getOrElse(time, 0.0)
      val m = (p + q) / 2.0

      // 计算KL(P||M)和KL(Q||M)
      val klPM = klDivergence(p, m)
      val klQM = klDivergence(q, m)

      sum + klPM + klQM
    } / 2.0

    // 打印结果到控制台
    println("======================================")
    println(s"通勤时长分布Jensen-Shannon散度计算结果")
    println(f"JSD值: $jsd")
    println("======================================")

    // 释放缓存
    synTimes.unpersist()
    comTimes.unpersist()
  }

  def CalJSD_chw(sc: SparkContext,
                 synDataPath: String,
                 commuteDataPath: String): Unit = {

    // 提取家区域、工作区域和通勤时长三元组
    def extractRegionTimeTriple(record: String): (String, String, Int) = {
      val parts = record.split(",")
      if (parts.length < 4) ("", "", 0) else {
        try {
          // 解析家区域和工作区域
          val homeRegion = parts(0).split(" ").lift(1).getOrElse("")
          val workRegion = parts(1).split(" ").lift(1).getOrElse("")

          // 解析出发时间和到达时间
          val departTime = parts(0).split(" ")(0).toInt
          val arriveTime = parts(1).split(" ")(0).toInt
          val commuteDiff = parts(3).toInt

          // 计算通勤时长：(到达时间-出发时间)*2 + 通勤差异
          val commuteTime = (arriveTime - departTime) * 2 + commuteDiff

          (homeRegion, workRegion, commuteTime)
        } catch {
          case _: Exception => ("", "", 0)
        }
      }
    }

    // 计算概率分布
    def calculateDistribution(rdd: RDD[(String, String, Int)]): Map[(String, String, Int), Double] = {
      val total = rdd.count().toDouble
      if (total == 0) Map.empty[(String, String, Int), Double] else {
        rdd.map(triple => (triple, 1.0 / total))
          .reduceByKey(_ + _)
          .collect()
          .toMap
      }
    }

    // 计算KL散度
    def klDivergence(p: Double, q: Double): Double = {
      if (p > 0 && q > 0) p * log(p / q)
      else 0.0
    }

    // 读取并处理synData数据
    val synTriples = sc.textFile(synDataPath)
      .map(extractRegionTimeTriple)
      .filter { case (h, w, t) => h.nonEmpty && w.nonEmpty && t > 0 }
      .persist()  // 缓存以提高性能

    val synDist = calculateDistribution(synTriples)
    println(s"SynData有效记录数: ${synTriples.count()}")
    println(s"SynData三元组类别数: ${synDist.size}")

    // 读取并处理commuteData数据
    val comTriples = sc.textFile(commuteDataPath)
      .map(extractRegionTimeTriple)
      .filter { case (h, w, t) => h.nonEmpty && w.nonEmpty && t > 0 }
      .persist()  // 缓存以提高性能

    val comDist = calculateDistribution(comTriples)
    println(s"CommuteData有效记录数: ${comTriples.count()}")
    println(s"CommuteData三元组类别数: ${comDist.size}")

    // 获取所有三元组
    val allTriples = (synDist.keySet ++ comDist.keySet).toSet
    println(s"总三元组类别数: ${allTriples.size}")

    // 计算JSD散度
    val jsd = allTriples.foldLeft(0.0) { (sum, triple) =>
      val p = synDist.getOrElse(triple, 0.0)
      val q = comDist.getOrElse(triple, 0.0)
      val m = (p + q) / 2.0

      // 计算KL(P||M)和KL(Q||M)
      val klPM = klDivergence(p, m)
      val klQM = klDivergence(q, m)

      sum + klPM + klQM
    } / 2.0

    // 打印结果到控制台
    println("======================================")
    println(s"通勤时长-家区域-工作区域联合分布JSD散度计算结果")
    println(f"JSD值: $jsd")
    println("======================================")

    // 释放缓存
    synTriples.unpersist()
    comTriples.unpersist()
  }

  def CalJSD_dw(sc: SparkContext,
                 synDataPath: String,
                 commuteDataPath: String): Unit = {

    // 提取工作区域和工作时长对
    def extractWorkRegionAndDuration(record: String): (String, Int) = {
      val parts = record.split(",")
      if (parts.length < 3) ("", 0) else {
        try {
          // 提取工作区域（第二个字段的第二个单词）
          val workRegion = parts(1).split(" ").lift(1).getOrElse("")

          // 提取工作时长（第三个字段）
          val workDuration = parts(2).toInt

          (workRegion, workDuration)
        } catch {
          case _: Exception => ("", 0)
        }
      }
    }

    // 计算概率分布
    def calculateDistribution(rdd: RDD[(String, Int)]): Map[(String, Int), Double] = {
      val total = rdd.count().toDouble
      if (total == 0) Map.empty[(String, Int), Double] else {
        rdd.map(pair => (pair, 1.0 / total))
          .reduceByKey(_ + _)
          .collect()
          .toMap
      }
    }

    // 计算KL散度
    def klDivergence(p: Double, q: Double): Double = {
      if (p > 0 && q > 0) p * log(p / q)
      else 0.0
    }

    // 读取并处理synData数据
    val synPairs = sc.textFile(synDataPath)
      .map(extractWorkRegionAndDuration)
      .filter { case (region, duration) => region.nonEmpty && duration > 0 }
      .persist()  // 缓存以提高性能

    val synDist = calculateDistribution(synPairs)
    println(s"SynData有效记录数: ${synPairs.count()}")
    println(s"SynData联合分布类别数: ${synDist.size}")

    // 读取并处理commuteData数据
    val comPairs = sc.textFile(commuteDataPath)
      .map(extractWorkRegionAndDuration)
      .filter { case (region, duration) => region.nonEmpty && duration > 0 }
      .persist()  // 缓存以提高性能

    val comDist = calculateDistribution(comPairs)
    println(s"CommuteData有效记录数: ${comPairs.count()}")
    println(s"CommuteData联合分布类别数: ${comDist.size}")

    // 获取所有键
    val allKeys = (synDist.keySet ++ comDist.keySet).toSet
    println(s"总联合分布类别数: ${allKeys.size}")

    // 计算JSD散度
    val jsd = allKeys.foldLeft(0.0) { (sum, key) =>
      val p = synDist.getOrElse(key, 0.0)
      val q = comDist.getOrElse(key, 0.0)
      val m = (p + q) / 2.0

      // 计算KL(P||M)和KL(Q||M)
      val klPM = klDivergence(p, m)
      val klQM = klDivergence(q, m)

      sum + klPM + klQM
    } / 2.0

    // 打印结果到控制台
    println("======================================")
    println(s"工作时长-工作区域联合分布JSD散度计算结果")
    println(f"JSD值: $jsd")
    println("======================================")

    // 释放缓存
    synPairs.unpersist()
    comPairs.unpersist()
  }

}