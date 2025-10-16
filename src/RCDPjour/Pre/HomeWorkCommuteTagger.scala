package RCDPjour.Pre

import scala.util.Try
import RCDPjour.others.GeneralFunctionSets
import org.apache.spark.SparkContext
import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone, Date}

object HomeWorkCommuteTagger {

  case class TripRecord(
                         id: String,
                         day: String,
                         timeStamp1: Int,  // 改为Int类型
                         ot: String,
                         oRegionId: Int,
                         oStation: String,
                         ox: Double,
                         oy: Double,
                         timeStamp2: Int,  // 改为Int类型
                         dt: String,
                         dRegionId: Int,
                         dStation: String,
                         dx: Double,
                         dy: Double,
                         hRegionId: Int,
                         wRegionId: Int
                       )

  def tagCommuteTrips(sc: SparkContext,
                      rawDataPath: String,
                      regionDataPath: String,
                      outputPath: String): Unit = {

    // 初始化区域选择器
    val regionSelector = new RegionSelector()
    regionSelector.initialize(sc, regionDataPath)

    // 读取并处理数据
    val commuteTrips = sc.textFile(rawDataPath)
      .map(_.split(","))
      .filter(hasValidHomeWorkLocations)  // 过滤有有效家庭和工作地坐标的记录
      .flatMap(createTripRecord(_, regionSelector))  // 转换为结构化记录
      .filter(record => isWithinTimeRange(record.timeStamp1) && isWithinTimeRange(record.timeStamp2))  // 过滤时间在5:00-23:00之间的记录
      .filter(isCommuteTrip)  // 筛选通勤行程
      .map(adjustTimestamps)  // 调整时间戳(减去30)
      .map(tagCommuteDirection)  // 标记通勤方向
      .sortBy(_.id)  // 按ID排序

    // 保存结果
    commuteTrips
      .map(_.toString)
      .coalesce(60)
      .saveAsTextFile(outputPath)
  }

  // 检查时间是否在5:00-23:00之间(原始时间戳范围)
  private def isWithinTimeRange(timestamp: Int): Boolean = {
    timestamp >= 30 && timestamp <= 138 // 5:00=30, 23:00=138
  }

  // 调整时间戳(减去30)
  private def adjustTimestamps(record: TripRecord): TripRecord = {
    record.copy(
      timeStamp1 = record.timeStamp1 - 30,
      timeStamp2 = record.timeStamp2 - 30
    )
  }


  private def hasValidHomeWorkLocations(columns: Array[String]): Boolean = {
    // 前置防御性检查：1. 数组不为空 2. 长度足够访问索引13 (需要至少14个元素)
    if (columns != null && columns.length >= 14) {
      // 安全访问所有索引
      columns(10).toDouble != 0.0 &&
        columns(11).toDouble != 0.0 &&
        columns(12).toDouble != 0.0 &&
        columns(13).toDouble != 0.0
    } else {
      // 数组长度不足或为空时，直接返回false
      false
    }
  }

  // 从日期字符串解析日期的函数
  private def parseDayFromDateColumn(dateString: String): Int = {
    try {
      // 日期格式为 "yyyy-MM-dd"
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
      dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
      val date = dateFormat.parse(dateString)

      val calendar = Calendar.getInstance(TimeZone.getTimeZone("Asia/Shanghai"))
      calendar.setTime(date)
      calendar.get(Calendar.DAY_OF_MONTH)
    } catch {
      case e: Exception =>
        // 如果解析失败，尝试从时间字符串解析
        try {
          val pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
          val dateFormat = new SimpleDateFormat(pattern)
          dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
          val date = dateFormat.parse(dateString)

          val calendar = Calendar.getInstance(TimeZone.getTimeZone("Asia/Shanghai"))
          calendar.setTime(date)
          calendar.get(Calendar.DAY_OF_MONTH)
        } catch {
          case _: Exception =>
            // 如果都失败，返回0
            0
        }
    }
  }

  // 正确解析北京时间
  private def parseBeijingTime(timeString: String): Int = {
    try {
      // 去掉末尾的'Z'，因为它实际是北京时间
      val cleanedTime = timeString.stripSuffix("Z")

      // 解析为北京时间
      val pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS"
      val beijingFormat = new SimpleDateFormat(pattern)
      beijingFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
      val date = beijingFormat.parse(cleanedTime)

      val calendar = Calendar.getInstance(TimeZone.getTimeZone("Asia/Shanghai"))
      calendar.setTime(date)
      val H = calendar.get(Calendar.HOUR_OF_DAY)
      val M = calendar.get(Calendar.MINUTE)

      // 计算时间戳：(小时*60 + 分钟)/10 + 1
      (H * 60 + M) / 10 + 1
    } catch {
      case e: Exception =>
        // 如果解析失败，尝试使用备用格式（不带毫秒）
        try {
          val patternWithoutMillis = "yyyy-MM-dd'T'HH:mm:ss"
          val backupFormat = new SimpleDateFormat(patternWithoutMillis)
          backupFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
          val date = backupFormat.parse(timeString.stripSuffix("Z"))

          val calendar = Calendar.getInstance(TimeZone.getTimeZone("Asia/Shanghai"))
          calendar.setTime(date)
          val H = calendar.get(Calendar.HOUR_OF_DAY)
          val M = calendar.get(Calendar.MINUTE)

          (H * 60 + M) / 10 + 1
        } catch {
          case _: Exception =>
            // 如果都失败，返回0
            0
        }
    }
  }

  // 创建结构化TripRecord
  private def createTripRecord(columns: Array[String], selector: RegionSelector): Option[TripRecord] = {
    try {
      // 使用第15列(索引14)作为日期源，并解析日期
      val day = parseDayFromDateColumn(columns(14)).toString

      Some(TripRecord(
        id = columns(0),
        day = day,
        timeStamp1 = parseBeijingTime(columns(1)),  // 使用自定义的北京时间解析函数
        ot = columns(1),
        oRegionId = selector.queryRegion(columns(3).toDouble, columns(4).toDouble),
        oStation = columns(2),
        ox = columns(3).toDouble,
        oy = columns(4).toDouble,
        timeStamp2 = parseBeijingTime(columns(5)),  // 使用自定义的北京时间解析函数
        dt = columns(5),
        dRegionId = selector.queryRegion(columns(7).toDouble, columns(8).toDouble),
        dStation = columns(6),
        dx = columns(7).toDouble,
        dy = columns(8).toDouble,
        hRegionId = selector.queryRegion(columns(11).toDouble, columns(10).toDouble),
        wRegionId = selector.queryRegion(columns(13).toDouble, columns(12).toDouble)
      ))
    } catch {
      case e: Exception =>
        // 打印异常信息以帮助调试
        println(s"Error parsing record: ${e.getMessage}")
        None  // 跳过格式错误的记录
    }
  }

  // 判断是否为通勤行程
  private def isCommuteTrip(record: TripRecord): Boolean = {
    (record.oRegionId == record.hRegionId && record.dRegionId == record.wRegionId) ||
      (record.oRegionId == record.wRegionId && record.dRegionId == record.hRegionId)
  }

  // 标记通勤方向
  private def tagCommuteDirection(record: TripRecord): TripRecordWithTag = {
    val commuteTag = if (record.oRegionId == record.hRegionId) 1 else 2
    TripRecordWithTag(record, commuteTag)
  }

  // 带通勤标记的记录类
  case class TripRecordWithTag(record: TripRecord, commuteTag: Int) {
    def id: String = record.id

    override def toString: String = {
      s"${record.id},${record.day},${record.timeStamp1},${record.ot},${record.oRegionId}," +
        s"${record.oStation},${record.ox},${record.oy},${record.timeStamp2},${record.dt}," +
        s"${record.dRegionId},${record.dStation},${record.dx},${record.dy}," +
        s"$commuteTag"
    }
  }
}