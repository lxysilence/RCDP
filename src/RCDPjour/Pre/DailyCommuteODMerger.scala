package RCDPjour.Pre

import org.apache.spark.SparkContext

object DailyCommuteODMerger {

  def DailyCommuteODMerge(sc: SparkContext,
                          ODDataPath: String,
                          outputPath: String): Unit = {
    // 定义常量表示字段索引，提高可读性
    val ID_INDEX = 0
    val DAY_INDEX = 1
    val TEMP1_INDEX = 2
    val TIME1_INDEX = 3
    val O_REGION_INDEX = 4
    val O_STATION_INDEX = 5
    val O_LNG_INDEX = 6
    val O_LAT_INDEX = 7
    val TEMP2_INDEX = 8
    val TIME2_INDEX = 9
    val D_REGION_INDEX = 10
    val D_STATION_INDEX = 11
    val D_LNG_INDEX = 12
    val D_LAT_INDEX = 13
    val TAG_INDEX = 14

    // 读取数据并处理
    val result = sc.textFile(ODDataPath)
      .map(_.split(",", -1)) // 只split一次
      .filter(_.length >= 15) // 确保有足够字段
      .map { parts =>
        // 根据tag值重构记录
        val reconstructed = parts(TAG_INDEX).toInt match {
          case 1 =>
            Array(
              parts(ID_INDEX), parts(DAY_INDEX),
              parts(O_REGION_INDEX), parts(O_STATION_INDEX),
              parts(O_LNG_INDEX), parts(O_LAT_INDEX),
              parts(D_REGION_INDEX), parts(D_STATION_INDEX),
              parts(D_LNG_INDEX), parts(D_LAT_INDEX),
              parts(TEMP1_INDEX), parts(TIME1_INDEX),
              parts(TEMP2_INDEX), parts(TIME2_INDEX),
              parts(TAG_INDEX)
            )
          case 2 =>
            Array(
              parts(ID_INDEX), parts(DAY_INDEX),
              parts(D_REGION_INDEX), parts(D_STATION_INDEX),
              parts(D_LNG_INDEX), parts(D_LAT_INDEX),
              parts(O_REGION_INDEX), parts(O_STATION_INDEX),
              parts(O_LNG_INDEX), parts(O_LAT_INDEX),
              parts(TEMP1_INDEX), parts(TIME1_INDEX),
              parts(TEMP2_INDEX), parts(TIME2_INDEX),
              parts(TAG_INDEX)
            )
        }
        // 返回重构后的记录和提取的5个关键字段
        (reconstructed, Array(
          reconstructed(10), // TEMP1
          reconstructed(11), // TIME1
          reconstructed(12), // TEMP2
          reconstructed(13), // TIME2
          reconstructed(14)  // TAG
        ))
      }
      // 按键分组：ID和Day
      .keyBy { case (reconstructed, _) =>
        (reconstructed(ID_INDEX), reconstructed(DAY_INDEX))
      }
      // 分组并处理每组数据
      .groupByKey()
      .flatMap { case ((id, day), records) =>
        // 将迭代器转换为列表以便多次使用
        val recordsList = records.toList

        // 检查记录数量是否满足要求
        // 每条记录贡献5个字段，加上基本字段共10个字段
        // 总字段数 = 10 + 5 * recordsList.size
        // 要求总字段数 >= 20 => 10 + 5n >= 20 => 5n >= 10 => n >= 2
        if (recordsList.size >= 2) {
          // 提取第一条记录的完整信息
          val firstRecord = recordsList.head._1

          // 提取家和工作地信息（前10个字段）
          val homeWorkInfo = firstRecord.slice(2, 10) // 索引2-9：家和工作地信息

          // 收集所有记录的关键字段（5个字段）
          val keyFields = recordsList.flatMap(_._2)

          // 构建最终结果数组
          val resultArray = Array(id, day) ++ homeWorkInfo ++ keyFields
          Some(resultArray)
        } else {
          None // 过滤掉不满足条件的记录
        }
      }
      // 筛选长度大于等于20的结果
      .filter(_.length >= 20)
      // 将数组转换为CSV行
      .map(_.mkString(","))

    // 保存结果
    result.coalesce(60).saveAsTextFile(outputPath)
  }
}