package RCDPjour.Pre

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object RCDPAdaptive {

  def CalRegionFlow(sc: SparkContext,
                    ODDataPath: String,
                    outputPath: String): Unit = {

    // 1. 读取OD数据
    val rawData = sc.textFile(ODDataPath)

    // 2. 处理每行数据，生成四个时间点记录
    val regionTimestamps: RDD[(String, Int)] = rawData.flatMap { line =>
      val fields = line.split(",")
      if (fields.length < 4) {
        None // 跳过字段不足的行
      } else {
        try {
          // 解析去程信息
          val outboundFields = fields(0).trim.split("\\s+")
          val departureTime = outboundFields(0).toInt
          val homeRegion = outboundFields(1)

          // 解析目的地信息
          val destinationFields = fields(1).trim.split("\\s+")
          val arrivalTime = destinationFields(0).toInt
          val workRegion = destinationFields(1)

          // 解析工作时长和往返差异
          val workDuration = fields(2).toInt
          val roundTripDiff = fields(3).toInt

          // 计算回程时间
          val returnDepartureTime = arrivalTime + workDuration
          val tripDuration = arrivalTime - departureTime
          val returnArrivalTime = returnDepartureTime + tripDuration + roundTripDiff

          // 生成四个时间点记录
          Seq(
            (homeRegion, departureTime),      // 家出发
            (workRegion, arrivalTime),        // 到达工作
            (workRegion, returnDepartureTime),// 工作出发
            (homeRegion, returnArrivalTime)   // 回到家
          )
        } catch {
          case e: Exception =>
            // 打印错误日志并跳过无效行
            println(s"数据处理错误: $line, 原因: ${e.getMessage}")
            None
        }
      }
    }

    // 2. 获取所有唯一区域编号
    val regions = regionTimestamps.keys.distinct().collect().sorted

    // 3. 生成完整的时间戳序列 (0-108)
    val allTimestamps = (0 to 108).toArray

    // 4. 为每个区域创建完整时间序列计数
    val regionCounts = regionTimestamps
      .map { case (region, timestamp) => ((region, timestamp), 1) }
      .reduceByKey(_ + _) // 计算实际出现的次数

    // 5. 广播区域列表以提高效率
    val broadcastRegions = sc.broadcast(regions)

    // 6. 创建完整的计数矩阵
    val completeCounts = regionCounts
      .map { case ((region, timestamp), count) => (region, (timestamp, count)) }
      .groupByKey() // 按区域分组
      .flatMap { case (region, timestamps) =>
        // 创建该区域的时间戳映射
        val countMap = timestamps.toMap
        // 为该区域生成所有时间戳的计数 (0-108)
        allTimestamps.map { ts =>
          (region, ts, countMap.getOrElse(ts, 0))
        }
      }

    // 7. 处理没有数据的区域
    val missingRegions = broadcastRegions.value.diff(
      completeCounts.map(_._1).distinct().collect()
    )

    val zeroCounts = sc.parallelize(missingRegions).flatMap { region =>
      allTimestamps.map(ts => (region, ts, 0))
    }

    // 8. 合并结果并排序
    val finalResult = completeCounts.union(zeroCounts)
      .sortBy(r => (r._1, r._2))
      .map { case (region, ts, count) => s"$region,$ts,$count" }

    // 9. 保存结果
    finalResult.coalesce(1).saveAsTextFile(outputPath)

    // 10. 打印样本信息
    println(s"总区域数: ${regions.length}")
    println("样本数据:")
    finalResult.take(5).foreach(println)
  }

  def AdaptiveTimeDiscretizer(sc: SparkContext,
                              ODDataPath: String,
                              indexPath: String,
                              outputPath: String): Unit = {

    // 1. 读取并解析索引映射规则
    val timeIndexMap = parseIndexFile(sc.textFile(indexPath))

    // 2. 广播映射规则到所有节点
    val broadcastMap = sc.broadcast(timeIndexMap)

    // 3. 处理OD数据
    val odData = sc.textFile(ODDataPath)
    val processedData = odData.map { line =>
      val fields = line.split(",")
      if (fields.length < 2) line // 字段不足则原样返回
      else {
        // 处理前两个字段
        val updatedFields = fields.take(2).map { field =>
          val parts = field.split("\\s+", 2) // 只分割成两部分
          if (parts.length == 2) {
            val originalTime = parts(0).toInt
            val newTime = lookupTimeIndex(originalTime, broadcastMap.value)
            s"$newTime ${parts(1)}" // 替换为新时间戳
          } else field // 格式错误则保留原字段
        }
        // 组合所有字段
        (updatedFields ++ fields.drop(2)).mkString(",")
      }
    }

    // 4. 保存结果
    processedData.coalesce(1).saveAsTextFile(outputPath)
  }

  // 解析索引文件为映射表
  private def parseIndexFile(indexData: RDD[String]): Map[Int, Int] = {
    indexData.flatMap { line =>
      val parts = line.split(",")
      if (parts.length == 2) {
        val timePart = parts(0).trim
        val newTime = parts(1).trim.toInt

        if (timePart.contains("-")) {
          // 处理范围格式 "25-30"
          val range = timePart.split("-")
          if (range.length == 2) {
            val start = range(0).toInt
            val end = range(1).toInt
            (start to end).map(t => (t, newTime))
          } else Nil
        } else {
          // 处理单个时间点
          try {
            val time = timePart.toInt
            Seq((time, newTime))
          } catch {
            case _: NumberFormatException => Nil
          }
        }
      } else Nil
    }.collect().toMap
  }

  // 查找时间戳对应的新索引
  private def lookupTimeIndex(originalTime: Int, indexMap: Map[Int, Int]): Int = {
    indexMap.getOrElse(originalTime, originalTime) // originalTime, originalTime) // 未找到映射则保留原值
  }
}