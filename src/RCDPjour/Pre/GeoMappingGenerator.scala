package RCDPjour.Pre

import org.apache.spark.SparkContext
import scala.math.BigDecimal

object GeoMappingGenerator {

  def GenerateGeoMap(sc: SparkContext,
                     ODDataPath: String,
                     outputPath: String): Unit = {

    // 读取原始数据文件
    val dataRDD = sc.textFile(ODDataPath)

    // 处理每行数据，提取并格式化经纬度对
    val geoRDD = dataRDD.flatMap { line =>
      try {
        val fields = line.split(",", -1)
        if (fields.length < 10) {
          None // 跳过字段不足的行
        } else {
          // 提取家和工作地的经纬度
          val homeLon = fields(3).trim
          val homeLat = fields(4).trim
          val workLon = fields(7).trim
          val workLat = fields(8).trim

          // 格式化为保留三位小数的字符串
          Seq(
            formatCoordinate(homeLon) + " " + formatCoordinate(homeLat),
            formatCoordinate(workLon) + " " + formatCoordinate(workLat)
          )
        }
      } catch {
        case e: Exception => None // 跳过格式错误的数据
      }
    }

    // 去重并排序
    val distinctGeoRDD = geoRDD.distinct().sortBy(identity)

    // 保存结果
    distinctGeoRDD.coalesce(1).saveAsTextFile(outputPath)
  }

  // 格式化坐标值，保留三位小数
  private def formatCoordinate(coord: String): String = {
    try {
      // 转换为Double并四舍五入到小数点后三位
      BigDecimal(coord.toDouble)
        .setScale(3, BigDecimal.RoundingMode.HALF_UP)
        .toString
    } catch {
      case e: NumberFormatException => "0.000" // 无效坐标默认值
    }
  }
}