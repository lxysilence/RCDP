package RCDPjour.Pre

import org.apache.spark.SparkContext

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object RCDPAblation {

  def GridLevel(sc: SparkContext,
                      ODDataPath: String,
                      IndexPath: String,
                      outputPath: String): Unit = {

    // 定义时间转换函数（将Z时间视为北京时间）
    def convertToTimeSlot(timeStr: String): Int = {
      // 移除末尾的'Z'并解析为LocalDateTime（无时区）
      val cleanTime = if (timeStr.endsWith("Z")) timeStr.dropRight(1) else timeStr
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")
      val dt = LocalDateTime.parse(cleanTime, formatter)

      // 计算从05:00开始的分钟数
      val minutesFrom5am =
        (dt.getHour - 5) * 60 +
          dt.getMinute

      // 转换为15分钟间隔序号（从1开始）
      (minutesFrom5am / 10) + 1
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

            // 转换四个关键时间点
            val depToWorkSlot = convertToTimeSlot(fields(10)) // 去程出发时间
            val arrAtWorkSlot = convertToTimeSlot(fields(12)) // 去程到达时间
            val depFromWorkSlot = convertToTimeSlot(fields(15)) // 回程出发时间
            val arrAtHomeSlot = convertToTimeSlot(fields(17)) // 回程到达时间

            // 计算时间指标
            val commuteTime = arrAtWorkSlot - depToWorkSlot
            val returnTime = arrAtHomeSlot - depFromWorkSlot
            val stayDuration = depFromWorkSlot - arrAtWorkSlot
            val timeDiff = commuteTime - returnTime

            // 格式：去程出发时间槽 家区域,去程到达时间槽 工作区域,停留时间,时间差
            Some(s"$depToWorkSlot $homeGeoId,$arrAtWorkSlot $workGeoId,$stayDuration,$timeDiff")
          }
        }
      } catch {
        case e: Exception => None  // 跳过格式错误的数据
      }
    }

    // 保存抽样结果
    resultRDD.coalesce(1).saveAsTextFile(outputPath)

  }
}