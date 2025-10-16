package RCDPjour.evaluation

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Change {

  def Adatrace(sc: SparkContext,
               commuteDataPath: String,
               outputDataPath: String): Unit = {

    // 1. 读取原始轨迹数据
    val rawData: RDD[String] = sc.textFile(commuteDataPath)

    // 2. 数据预处理
    val processedData: RDD[String] = rawData
      .filter(line => !line.startsWith("#"))  // 删除以#开头的行
      .filter(line => line.contains(">0:"))   // 只保留包含轨迹数据的行
      .map(line => line.replace(">0:", ""))    // 删除每行的>0:前缀
      .map(line => {
        // 以分号分割字段并过滤空字段
        val points = line.split(";").filter(_.nonEmpty)

        if (points.nonEmpty) {
          // 提取第一个点
          val firstPoint = points.head.split(",")
          val startLon = "%.2f".format(firstPoint(0).toDouble)
          val startLat = "%.2f".format(firstPoint(1).toDouble)

          // 提取最后一个点
          val lastPoint = points.last.split(",")
          val endLon = "%.2f".format(lastPoint(0).toDouble)
          val endLat = "%.2f".format(lastPoint(1).toDouble)

          // 格式化为: 起点经度,起点纬度;终点经度,终点纬度;
          s"$startLon $startLat,$endLon $endLat"
        } else {
          "" // 空行处理
        }
      })
      .filter(_.nonEmpty)  // 过滤掉空行

    // 3. 保存处理后的数据
    processedData.coalesce(1).saveAsTextFile(outputDataPath)

    // 可选：打印一些样本数据用于验证
    println("Processed data sample:")
    processedData.take(5).foreach(println)
  }
}