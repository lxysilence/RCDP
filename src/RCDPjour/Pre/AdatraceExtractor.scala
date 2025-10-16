package RCDPjour.Pre

import org.apache.spark.SparkContext

object AdatraceExtractor {

  def ExtractDailyCommute(sc: SparkContext,
                          ODDataPath: String,
                          outputPath: String): Unit = {

    // 读取原始数据
    val rawData = sc.textFile(ODDataPath)

    // 处理每行数据
    val processedData = rawData.map { line =>
      // 使用逗号分割字段
      val fields = line.split(",")
      // 提取家经度(索引3)和家纬度(索引4)
      val homeLon = fields(3)
      val homeLat = fields(4)
      // 提取工作经度(索引7)和工作纬度(索引8)
      val workLon = fields(7)
      val workLat = fields(8)

      // 构建输出格式：>0:家经纬度;工作经纬度
      s">0:$homeLon,$homeLat;$workLon,$workLat"
    }

    val result=processedData.zipWithIndex() // 生成(原始内容, 行号)
      .flatMap { case (lineContent, lineNumber) =>
        // 生成两行：注释行和原始行
        Seq(
          s"#$lineNumber:",    // 添加注释行
          lineContent          // 保留原始内容
        )
      }
    result.coalesce(1).saveAsTextFile(outputPath)
  }
}