package RCDPjour.Pre

import org.apache.spark.SparkContext

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object DailyCommuteFilter_Chen_15min {

  def CalDailyCommute(sc: SparkContext,
                      ODDataPath: String,
                      outputPath: String): Unit = {

    val result = sc.textFile(ODDataPath)
      .map(_.split(",", -1))
      .filter(fields => fields.length >= 19) // 确保有19个字段
      .keyBy(fields => fields(0))           // 使用ID作为key
      .reduceByKey((a, b) => a)             // 按ID去重
      .map(_._2)                            // 获取去重后的记录
      .flatMap { fields =>                 // 使用flatMap过滤无效记录
        try {
          val hid = fields(2)   // 家区域ID
          val wid = fields(6)   // 工作区域ID

          // 时间转换函数（将Z时间视为北京时间）
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
          Some(s"$depToWorkSlot $hid,$arrAtWorkSlot $wid,$stayDuration,$timeDiff")
        } catch {
          case e: Exception =>
            println(s"Error processing record: ${fields.mkString(",")}. Reason: ${e.getMessage}")
            None
        }
      }

    // 保存结果
    result.coalesce(1).saveAsTextFile(outputPath)
  }
}