package RCDPjour.Pre

import org.apache.spark.SparkContext

object DailyCommuteODMerger_filter {

  def DailyCommutefilter(sc: SparkContext,
                         ODDataPath: String,
                         outputPath: String): Unit = {

    // 定义常量表示字段索引（使用更清晰的命名）
    val TotalFields = 20       // 总字段数
    val TagFieldIndex = 14     // 第15字段（索引14）标签字段
    val EndTagFieldIndex = 19  // 第20字段（索引19）结束标签字段
    val StartTimeFieldIndex = 12 // 第13字段（索引12）起始时间字段
    val EndTimeFieldIndex = 15  // 第16字段（索引15）结束时间字段
    val IdFieldIndex = 0       // 第一字段索引（ID）
    val RemoveFieldIndex = 1    // 需要移除的字段索引（第二个字段）

    // 定义时间比较函数（可以访问本地常量）
    def isValidTimeComparison(parts: Array[String]): Boolean = {
      try {
        val startTime = parts(StartTimeFieldIndex).toDouble
        val endTime = parts(EndTimeFieldIndex).toDouble
        startTime < endTime
      } catch {
        case _: NumberFormatException => false
      }
    }

    // 定义移除字段函数
    def removeField(parts: Array[String]): Array[String] = {
      (parts.take(RemoveFieldIndex) ++ parts.drop(RemoveFieldIndex + 1))
    }

    // 读取数据并处理
    val filteredData = sc.textFile(ODDataPath)
      // 分割字段并筛选长度
      .map(_.split(",", TotalFields))
      .filter(_.length == TotalFields)

      // 应用业务筛选条件
      .filter { parts =>
        parts(TagFieldIndex) == "1" &&      // 标签字段为1
          parts(EndTagFieldIndex) == "2" &&    // 结束标签字段为2
          isValidTimeComparison(parts)         // 时间字段比较
      }

      // 移除日期并准备去重
      .map { parts =>
        val newParts = removeField(parts)
        (parts(IdFieldIndex), newParts)      // (ID, 处理后的数组)
      }
      .reduceByKey { (first, _) => first } // 相同ID时保留第一个出现的记录
      .values                                // 只保留处理后的数组

      // 转换为CSV行
      .map(_.mkString(","))

    // 保存结果
    filteredData.coalesce(1).saveAsTextFile(outputPath)
  }
}