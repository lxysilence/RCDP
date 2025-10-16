package RCDPjour.evaluation

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object KT {
  def AdaKT_h(sc: SparkContext,
              synDataPath: String,
              commuteDataPath: String): Unit = {

    // 读取并处理synData数据
    val synCounts = sc.textFile(synDataPath)
      .map(_.split(","))
      .filter(_.length == 2)
      .map(parts => (parts(0), 1))
      .reduceByKey(_ + _)

    // 读取并处理commuteData数据
    val comCounts = sc.textFile(commuteDataPath)
      .map(_.split(","))
      .filter(_.length == 2)
      .map(parts => (parts(0), 1))

      .reduceByKey(_ + _)

    // 获取共同区域并构建排名序列
    val commonRegions = comCounts.map(_._1)
      .intersection(synCounts.map(_._1))
      .collect()
      .sorted  // 确保区域顺序一致

    // 获取共同区域的计数映射
    val comCountMap = comCounts.collectAsMap()
    val synCountMap = synCounts.collectAsMap()

    // 构建排名序列 (区域 -> 排名)
    val comRankings = comCounts
      .sortBy(-_._2) // 按计数降序
      .zipWithIndex() // 添加排名
      .map { case ((region, _), idx) => (region, idx) }
      .collectAsMap()

    val synRankings = synCounts
      .sortBy(-_._2)
      .zipWithIndex()
      .map { case ((region, _), idx) => (region, idx) }
      .collectAsMap()

    // 计算KT系数
    var concordant = 0
    var discordant = 0
    val n = commonRegions.length

    for (i <- 0 until n; j <- i + 1 until n) {
      val regionA = commonRegions(i)
      val regionB = commonRegions(j)

      // 获取两个区域在两个数据集中的排名
      val comRankA = comRankings(regionA)
      val comRankB = comRankings(regionB)
      val synRankA = synRankings(regionA)
      val synRankB = synRankings(regionB)

      // 比较排名顺序
      val comOrder = comRankA.compareTo(comRankB)
      val synOrder = synRankA.compareTo(synRankB)

      if (comOrder * synOrder > 0) {
        concordant += 1
      } else if (comOrder * synOrder < 0) {
        discordant += 1
      }
      // 相等情况忽略不计（标准KT处理）
    }

    // 计算KT系数
    val kt = if (n < 2) 0.0 else {
      (concordant - discordant).toDouble / (n * (n - 1) / 2)
    }

    println("======================================")
    println(s"***** Kendall Tau for Home Regions: $kt *****")
    println("======================================")
  }

  def AdaKT_w(sc: SparkContext,
              synDataPath: String,
              commuteDataPath: String): Unit = {

    // 读取并处理synData数据
    val synCounts = sc.textFile(synDataPath)
      .map(_.split(","))
      .filter(_.length == 2)
      .map(parts => (parts(1), 1))
      .reduceByKey(_ + _)

    // 读取并处理commuteData数据
    val comCounts = sc.textFile(commuteDataPath)
      .map(_.split(","))
      .filter(_.length == 2)
      .map(parts => (parts(1), 1))
      .reduceByKey(_ + _)

    // 获取共同区域并构建排名序列
    val commonRegions = comCounts.map(_._1)
      .intersection(synCounts.map(_._1))
      .collect()
      .sorted  // 确保区域顺序一致

    // 获取共同区域的计数映射
    val comCountMap = comCounts.collectAsMap()
    val synCountMap = synCounts.collectAsMap()

    // 构建排名序列 (区域 -> 排名)
    val comRankings = comCounts
      .sortBy(-_._2) // 按计数降序
      .zipWithIndex() // 添加排名
      .map { case ((region, _), idx) => (region, idx) }
      .collectAsMap()

    val synRankings = synCounts
      .sortBy(-_._2)
      .zipWithIndex()
      .map { case ((region, _), idx) => (region, idx) }
      .collectAsMap()

    // 计算KT系数
    var concordant = 0
    var discordant = 0
    val n = commonRegions.length

    for (i <- 0 until n; j <- i + 1 until n) {
      val regionA = commonRegions(i)
      val regionB = commonRegions(j)

      // 获取两个区域在两个数据集中的排名
      val comRankA = comRankings(regionA)
      val comRankB = comRankings(regionB)
      val synRankA = synRankings(regionA)
      val synRankB = synRankings(regionB)

      // 比较排名顺序
      val comOrder = comRankA.compareTo(comRankB)
      val synOrder = synRankA.compareTo(synRankB)

      if (comOrder * synOrder > 0) {
        concordant += 1
      } else if (comOrder * synOrder < 0) {
        discordant += 1
      }
      // 相等情况忽略不计（标准KT处理）
    }

    // 计算KT系数
    val kt = if (n < 2) 0.0 else {
      (concordant - discordant).toDouble / (n * (n - 1) / 2)
    }

    println("======================================")
    println(s"***** Kendall Tau for Work Regions: $kt *****")
    println("======================================")
  }





  def CalKT_h(sc: SparkContext,
           synDataPath: String,
           commuteDataPath: String): Unit = {

    // 读取并处理synData数据
    val synCounts = sc.textFile(synDataPath)
      .map(_.split(","))
      .filter(_.length == 4)
      .map(parts => (parts(0).split(" ")(1), 1)) // (家区域, 1)
      .reduceByKey(_ + _) // 统计每个家区域的出现次数
      .sortBy(-_._2) // 按计数降序排序

    // 读取并处理commuteData数据
    val comCounts = sc.textFile(commuteDataPath)
      .map(_.split(","))
      .filter(_.length == 4)
      .map(parts => (parts(0).split(" ")(1), 1)) // (家区域, 1)
      .reduceByKey(_ + _) // 统计每个家区域的出现次数
      .sortBy(-_._2) // 按计数降序排序

    // 取commuteData中前150的家区域
    val top150Regions = comCounts.take(150).map(_._1).toSet
    val broadcastTop150 = sc.broadcast(top150Regions)

    // 过滤出两个数据集中都存在的家区域
    val filteredSyn = synCounts.filter { case (region, _) =>
      broadcastTop150.value.contains(region)
    }.collect().toMap

    val filteredCom = comCounts.filter { case (region, _) =>
      broadcastTop150.value.contains(region)
    }.collect().toMap

    // 获取共同存在的家区域列表
    val commonRegions = filteredSyn.keySet.intersect(filteredCom.keySet).toArray

    // 计算KT系数
    val n = commonRegions.length
    var concordant = 0
    var discordant = 0

    for (i <- 0 until n; j <- i+1 until n) {
      val a = commonRegions(i)
      val b = commonRegions(j)

      val comA = filteredCom(a)
      val comB = filteredCom(b)
      val synA = filteredSyn(a)
      val synB = filteredSyn(b)

      val comOrder = comA.compareTo(comB)
      val synOrder = synA.compareTo(synB)

      if (comOrder * synOrder > 0) {
        concordant += 1
      } else if (comOrder * synOrder < 0) {
        discordant += 1
      }else if (comOrder==0 && synOrder == 0) {
        concordant += 1
      }else{
        discordant += 1
      }
    }

    // 计算KT系数
    val kt = if (n < 2) 0.0
    else (concordant - discordant).toDouble / (n * (n - 1) / 2)

    // 打印结果到控制台
    println("======================================")
    println(s"Home KT Coefficient: $kt")
    println(s"Concordant pairs: $concordant")
    println(s"Discordant pairs: $discordant")
    println(s"Total comparable pairs: ${n*(n-1)/2}")
    println("======================================")
  }

  def CalKT_w(sc: SparkContext,
              synDataPath: String,
              commuteDataPath: String): Unit = {

    // 读取并处理synData数据
    val synCounts = sc.textFile(synDataPath)
      .map(_.split(","))
      .filter(_.length == 4)
      .map(parts => (parts(1).split(" ")(1), 1)) // (工作区域, 1)
      .reduceByKey(_ + _) // 统计每个工作区域的出现次数
      .sortBy(-_._2) // 按计数降序排序

    // 读取并处理commuteData数据
    val comCounts = sc.textFile(commuteDataPath)
      .map(_.split(","))
      .filter(_.length == 4)
      .map(parts => (parts(1).split(" ")(1), 1)) // (工作区域, 1)
      .reduceByKey(_ + _) // 统计每个工作区域的出现次数
      .sortBy(-_._2) // 按计数降序排序

    // 取commuteData中前150的工作区域
    val top150Regions = comCounts.take(150).map(_._1).toSet
    val broadcastTop150 = sc.broadcast(top150Regions)

    // 过滤出两个数据集中都存在的工作区域
    val filteredSyn = synCounts.filter { case (region, _) =>
      broadcastTop150.value.contains(region)
    }.collect().toMap

    val filteredCom = comCounts.filter { case (region, _) =>
      broadcastTop150.value.contains(region)
    }.collect().toMap

    // 获取共同存在的工作区域列表
    val commonRegions = filteredSyn.keySet.intersect(filteredCom.keySet).toArray

    // 计算KT系数
    val n = commonRegions.length
    var concordant = 0
    var discordant = 0

    for (i <- 0 until n; j <- i+1 until n) {
      val a = commonRegions(i)
      val b = commonRegions(j)

      val comA = filteredCom(a)
      val comB = filteredCom(b)
      val synA = filteredSyn(a)
      val synB = filteredSyn(b)

      val comOrder = comA.compareTo(comB)
      val synOrder = synA.compareTo(synB)

      if (comOrder * synOrder > 0) {
        concordant += 1
      } else if (comOrder * synOrder < 0) {
        discordant += 1
      }else if (comOrder==0 && synOrder == 0) {
        concordant += 1
      }else{
        discordant += 1
      }
    }

    // 计算KT系数
    val kt = if (n < 2) 0.0
    else (concordant - discordant).toDouble / (n * (n - 1) / 2)

    // 打印结果到控制台
    println("======================================")
    println(s"Work KT Coefficient: $kt")
    println(s"Concordant pairs: $concordant")
    println(s"Discordant pairs: $discordant")
    println(s"Total comparable pairs: ${n*(n-1)/2}")
    println("======================================")
  }
}