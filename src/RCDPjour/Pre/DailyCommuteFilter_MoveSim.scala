package RCDPjour.Pre

import org.apache.spark.SparkContext
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.math.BigDecimal

object DailyCommuteFilter_MoveSim {

  def CalDailyCommute(sc: SparkContext,
                      ODDataPath: String,
                      IndexPath: String,
                      outputPath1: String,
                      outputPath2: String): Unit = {

    // 定义时间转换函数（将Z时间视为北京时间）
    def convertToTimeSlot(timeStr: String): Int = {
      // 移除末尾的'Z'并解析为LocalDateTime
      val cleanTime = if (timeStr.endsWith("Z")) timeStr.dropRight(1) else timeStr
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")
      val dt = LocalDateTime.parse(cleanTime, formatter)

      // 计算从00:00开始的分钟数
      val minutesFrom5am = dt.getHour * 60 + dt.getMinute

      // 转换为30分钟间隔序号（从1开始）
      (minutesFrom5am / 30) + 1
    }

    // 坐标格式化函数（保留三位小数）
    def formatCoordinate(coord: String): String = {
      try {
        BigDecimal(coord.toDouble)
          .setScale(3, BigDecimal.RoundingMode.HALF_UP)
          .toString
      } catch {
        case e: NumberFormatException => "0.000"
      }
    }

    // 构建地理编号映射（行号从1开始）
    val geoIndexMap = sc.textFile(IndexPath)
      .zipWithIndex()
      .map { case (line, index) =>
        (line.trim, (index + 1).toInt)  // 行号从1开始
      }
      .collect()
      .toMap

    // 广播地理映射到所有节点
    val broadcastGeoMap = sc.broadcast(geoIndexMap)

    // 处理OD数据
    val resultRDD = sc.textFile(ODDataPath).flatMap { line =>
      try {
        val fields = line.split(",", -1)
        if (fields.length < 19) None  // 跳过字段不足的行
        else {
          // 提取家和工作地经纬度
          val homeLon = formatCoordinate(fields(3).trim)
          val homeLat = formatCoordinate(fields(4).trim)
          val workLon = formatCoordinate(fields(7).trim)
          val workLat = formatCoordinate(fields(8).trim)

          // 创建经纬度字符串
          val homeGeoStr = s"$homeLon $homeLat"
          val workGeoStr = s"$workLon $workLat"

          // 获取地理编号
          val geoMap = broadcastGeoMap.value
          val homeGeoId = geoMap.getOrElse(homeGeoStr, -1)
          val workGeoId = geoMap.getOrElse(workGeoStr, -1)

          // 如果地理编号无效则跳过
          if (homeGeoId == -1 || workGeoId == -1) None
          else {
            // 提取时间字段
            val depToWorkTime = fields(10).trim  // 去程出发时间
            val arrAtWorkTime = fields(12).trim  // 去程到达时间
            val depFromWorkTime = fields(15).trim  // 回程出发时间
            val arrAtHomeTime = fields(17).trim  // 回程到达时间

            // 转换为时间槽
            val timeSlot1 = convertToTimeSlot(depToWorkTime)
            val timeSlot2 = convertToTimeSlot(arrAtWorkTime)
            val timeSlot3 = convertToTimeSlot(depFromWorkTime)
            val timeSlot4 = convertToTimeSlot(arrAtHomeTime)

            // 构建输出记录
            Some(s"$timeSlot1 $homeGeoId,$timeSlot2 $workGeoId,$timeSlot3 $workGeoId,$timeSlot4 $homeGeoId")
          }
        }
      } catch {
        case e: Exception => None  // 跳过格式错误的数据
      }
    }


    // 随机抽取170000行数据
    val sampleSize = 170000
    val sampledRDD = {
      val totalCount = resultRDD.count()
      if (totalCount <= sampleSize) {
        resultRDD  // 如果数据不足170000行，直接使用全部数据
      } else {
        val fraction = sampleSize.toDouble / totalCount * 1.1 // 稍微多采样一些
        resultRDD.sample(withReplacement = false, fraction)
          .zipWithIndex()
          .filter(_._2 < sampleSize)
          .map(_._1)
      }
    }


    // 在原有代码的resultRDD处理之后添加以下代码

    // 处理转换每条记录为48个字段
    val finalRDD = sampledRDD.flatMap { record =>
      try {
        val fields = record.split(",")
        if (fields.length != 4) None // 跳过格式不正确的行
        else {
          // 解析四个事件的时间槽和位置
          val Array(leaveHomeTime, leaveHomeLoc) = fields(0).split(" ")
          val Array(arriveWorkTime, arriveWorkLoc) = fields(1).split(" ")
          val Array(leaveWorkTime, leaveWorkLoc) = fields(2).split(" ")
          val Array(arriveHomeTime, arriveHomeLoc) = fields(3).split(" ")

          // 转换为整数
          val t1 = leaveHomeTime.toInt
          val t4 = arriveHomeTime.toInt
          val homeLoc = leaveHomeLoc
          val workLoc = arriveWorkLoc

          // 验证位置一致性
          if (homeLoc != arriveHomeLoc || workLoc != leaveWorkLoc) None
          else {
            // 创建48个时间槽的数组，初始化为0
            val timeSlots = Array.fill(48)("0")

            // 设置家位置范围 (第一个字段到t1，以及t4到48)
            for (i <- 0 until t1) {
              timeSlots(i) = homeLoc
            }
            for (i <- t4 - 1 until 48) {
              timeSlots(i) = homeLoc
            }

            // 设置工作位置范围 (t1到t4之间)
            for (i <- t1 until t4 - 1) {
              timeSlots(i) = workLoc
            }

            // 返回48个字段的字符串
            Some(timeSlots.mkString(" "))
          }
        }
      } catch {
        case e: Exception => None // 跳过格式错误的数据
      }
    }
    // 保存抽样结果
    sampledRDD.coalesce(1).saveAsTextFile(outputPath1)
    // 保存最终结果
    finalRDD.coalesce(1).saveAsTextFile(outputPath2)

  }



  def Change(sc: SparkContext,
                      innputDataPath: String,
                      outputPath: String): Unit = {

    // 定义时间段范围 (5:00-22:30)
    val startIndex = 10  // 5:00对应索引10
    val endIndex = 45    // 22:30对应索引45

    // 读取输入数据
    val data = sc.textFile(innputDataPath)

    // 处理每行数据
    val results = data.map { line =>
      // 分割字段（可能有多个空格分隔）
      val fields = line.trim.split("\\s+")

      // 提取5:00-22:30时间段的数据
      val timeWindow = fields.slice(startIndex, endIndex + 1)

      // 统计区域出现频率
      val regionFreq = timeWindow.groupBy(identity).mapValues(_.length)

      // 获取出现频率最高的两个区域
      val topRegions = regionFreq.toSeq.sortBy(-_._2).take(2).map(_._1).toSet

      // 生成输出字段 (索引位置转换为时间编号)
      val outputFields = timeWindow.zipWithIndex.collect {
        case (region, idx) if topRegions.contains(region) =>
          val timeId = startIndex + idx - 9  // 计算时间编号
          s"$timeId $region"
      }

      // 返回逗号分隔的结果
      outputFields.mkString(",")
    }

    // 保存结果
    results.coalesce(1).saveAsTextFile(outputPath)
  }
}