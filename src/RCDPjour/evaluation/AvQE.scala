
package RCDPjour.evaluation

import org.apache.spark.SparkContext


object AvQE {

  def CalAvQE_h(sc: SparkContext,
                synDataPath: String,
                commuteDataPath: String): Unit = {

    // 读取并处理synData数据
    val synData = sc.textFile(synDataPath)
      .map(_.split(","))
      .filter(_.length == 4)
      .map { parts =>
        val homeRegion = parts(0).split(" ")(1) // 提取家区域
        (homeRegion, 1)
      }
      .reduceByKey(_ + _) // 统计每个家区域的出现次数

    // 读取并处理commuteData数据
    val commuteData = sc.textFile(commuteDataPath)
      .map(_.split(","))
      .filter(_.length == 4)
      .map { parts =>
        val homeRegion = parts(0).split(" ")(1) // 提取家区域
        (homeRegion, 1)
      }
      .reduceByKey(_ + _) // 统计每个家区域的出现次数

    // 全外连接，确保所有家区域都被包含
    val joinedCounts = commuteData.fullOuterJoin(synData)
      .map { case (homeRegion, (comCountOpt, synCountOpt)) =>
        val comCount = comCountOpt.getOrElse(0)
        val synCount = synCountOpt.getOrElse(0)
        (homeRegion, comCount, synCount)
      }

    // 计算相对误差
    val errorRDD = joinedCounts.map { case (homeRegion, comCount, synCount) =>
      val denominator = scala.math.max(comCount.toDouble, 2000)
      val relativeError = math.abs(synCount.toDouble - comCount.toDouble) / denominator
      (homeRegion, comCount, synCount, relativeError)
    }

    // 尝试采样500行，如果数据不足则使用全部数据
    val sampleSize = 500L
    val totalCount = errorRDD.count()
    val sampleFraction = if (totalCount > sampleSize) sampleSize.toDouble / totalCount else 1.0

    val sampledErrors = errorRDD.sample(withReplacement = false, fraction = sampleFraction)

    // 计算平均相对误差
    val avgError = if (sampledErrors.isEmpty()) {
      0.0
    } else {
      val (sum, count) = sampledErrors.map { case (_, _, _, error) =>
        (error, 1L)
      }.reduce { case ((sum1, count1), (sum2, count2)) =>
        (sum1 + sum2, count1 + count2)
      }
      sum / count
    }

    // 打印结果到控制台
    println("======================================")
    println(s"****************Average Relative Error homeRegion: $avgError****************")
    println("======================================")
  }

    def CalAvQE_w(sc: SparkContext,
                  synDataPath: String,
                  commuteDataPath: String): Unit = {

      // 读取并处理synData数据（提取工作区域）
      val synData = sc.textFile(synDataPath)
        .map(_.split(","))
        .filter(_.length == 4)
        .map { parts =>
          val workRegion = parts(1).split(" ")(1) // 提取工作区域（第二个字段的第二个单词）
          (workRegion, 1)
        }
        .reduceByKey(_ + _) // 统计每个工作区域的出现次数

      // 读取并处理commuteData数据（提取工作区域）
      val commuteData = sc.textFile(commuteDataPath)
        .map(_.split(","))
        .filter(_.length == 4)
        .map { parts =>
          val workRegion = parts(1).split(" ")(1) // 提取工作区域
          (workRegion, 1)
        }
        .reduceByKey(_ + _) // 统计每个工作区域的出现次数

      // 全外连接，确保所有工作区域都被包含
      val joinedCounts = commuteData.fullOuterJoin(synData)
        .map { case (workRegion, (comCountOpt, synCountOpt)) =>
          val comCount = comCountOpt.getOrElse(0)
          val synCount = synCountOpt.getOrElse(0)
          (workRegion, comCount, synCount)
        }

      // 计算相对误差
      val errorRDD = joinedCounts.map { case (workRegion, comCount, synCount) =>
        val denominator = scala.math.max(comCount.toDouble, 2000)
        val relativeError = math.abs(synCount.toDouble - comCount.toDouble) / denominator
        (workRegion, comCount, synCount, relativeError)
      }

      // 尝试采样500行，如果数据不足则使用全部数据
      val sampleSize = 500L
      val totalCount = errorRDD.count()
      val sampleFraction = if (totalCount > sampleSize) sampleSize.toDouble / totalCount else 1.0

      val sampledErrors = errorRDD.sample(withReplacement = false, fraction = sampleFraction)

      // 计算平均相对误差
      val avgError = if (sampledErrors.isEmpty()) {
        0.0
      } else {
        val (sum, count) = sampledErrors.map { case (_, _, _, error) =>
          (error, 1L)
        }.reduce { case ((sum1, count1), (sum2, count2)) =>
          (sum1 + sum2, count1 + count2)
        }
        sum / count
      }

      // 打印结果到控制台
      println("======================================")
      println(s"*********************Average Work Region Error: $avgError*************")
      println("======================================")
    }

  def CalAvQE_s(sc: SparkContext,
                synDataPath: String,
                commuteDataPath: String): Unit = {

    // 读取并处理synData数据（提取开始工作时间）
    val synData = sc.textFile(synDataPath)
      .map(_.split(","))
      .filter(_.length == 4)
      .map { parts =>
        val startTime = parts(1).split(" ")(0) // 提取开始工作时间（第二个字段的第一个单词）
        (startTime, 1)
      }
      .reduceByKey(_ + _) // 统计每个开始时间的出现次数

    // 读取并处理commuteData数据（提取开始时间）
    val commuteData = sc.textFile(commuteDataPath)
      .map(_.split(","))
      .filter(_.length == 4)
      .map { parts =>
        val startTime = parts(1).split(" ")(0) // 提取工作区域
        (startTime, 1)
      }
      .reduceByKey(_ + _) // 统计每个工作区域的出现次数

    // 全外连接，确保所有工作区域都被包含
    val joinedCounts = commuteData.fullOuterJoin(synData)
      .map { case (startTime, (comCountOpt, synCountOpt)) =>
        val comCount = comCountOpt.getOrElse(0)
        val synCount = synCountOpt.getOrElse(0)
        (startTime, comCount, synCount)
      }

    // 计算相对误差
    val errorRDD = joinedCounts.map { case (startTime, comCount, synCount) =>
      val denominator = scala.math.max(comCount.toDouble, 40000)
      val relativeError = math.abs(synCount.toDouble - comCount.toDouble) / denominator
      (startTime, comCount, synCount, relativeError)
    }

    // 尝试采样500行，如果数据不足则使用全部数据
    val sampleSize = 500L
    val totalCount = errorRDD.count()
    val sampleFraction = if (totalCount > sampleSize) sampleSize.toDouble / totalCount else 1.0

    val sampledErrors = errorRDD.sample(withReplacement = false, fraction = sampleFraction)

    // 计算平均相对误差
    val avgError = if (sampledErrors.isEmpty()) {
      0.0
    } else {
      val (sum, count) = sampledErrors.map { case (_, _, _, error) =>
        (error, 1L)
      }.reduce { case ((sum1, count1), (sum2, count2)) =>
        (sum1 + sum2, count1 + count2)
      }
      sum / count
    }

    // 打印结果到控制台
    println("======================================")
    println(s"*******************Average Start Time Error: $avgError*****************")
    println("======================================")
  }

  def CalAvQE_f(sc: SparkContext,
                synDataPath: String,
                commuteDataPath: String): Unit = {

    // 读取并处理synData数据（提取结束工作时间）
    val synData = sc.textFile(synDataPath)
      .map(_.split(","))
      .filter(_.length == 4)
      .map { parts =>
        val startTime = parts(1).split(" ")(0).toInt // 提取开始工作时间（第二个字段的第一个单词）
        val workDuration = parts(2) .toInt //工作时长
        val finishTime = startTime + workDuration //结束工作时间
        (finishTime, 1)
      }
      .reduceByKey(_ + _) // 统计每个开始时间的出现次数

    // 读取并处理commuteData数据（提取开始时间）
    val commuteData = sc.textFile(commuteDataPath)
      .map(_.split(","))
      .filter(_.length == 4)
      .map { parts =>
        val startTime = parts(1).split(" ")(0).toInt // 提取工作区域
        val workDuration = parts(2).toInt
        val finishTime = startTime + workDuration
        (finishTime, 1)
      }
      .reduceByKey(_ + _) // 统计每个工作区域的出现次数

    // 全外连接，确保所有工作区域都被包含
    val joinedCounts = commuteData.fullOuterJoin(synData)
      .map { case (finishTime, (comCountOpt, synCountOpt)) =>
        val comCount = comCountOpt.getOrElse(0)
        val synCount = synCountOpt.getOrElse(0)
        (finishTime, comCount, synCount)
      }

    // 计算相对误差
    val errorRDD = joinedCounts.map { case (finishTime, comCount, synCount) =>
      val denominator = scala.math.max(comCount.toDouble, 40000)
      val relativeError = math.abs(synCount.toDouble - comCount.toDouble) / denominator
      (finishTime, comCount, synCount, relativeError)
    }

    // 尝试采样500行，如果数据不足则使用全部数据
    val sampleSize = 500L
    val totalCount = errorRDD.count()
    val sampleFraction = if (totalCount > sampleSize) sampleSize.toDouble / totalCount else 1.0

    val sampledErrors = errorRDD.sample(withReplacement = false, fraction = sampleFraction)

    // 计算平均相对误差
    val avgError = if (sampledErrors.isEmpty()) {
      0.0
    } else {
      val (sum, count) = sampledErrors.map { case (_, _, _, error) =>
        (error, 1L)
      }.reduce { case ((sum1, count1), (sum2, count2)) =>
        (sum1 + sum2, count1 + count2)
      }
      sum / count
    }

    // 打印结果到控制台
    println("======================================")
    println(s"***************Average finish time Error: $avgError******************")
    println("======================================")
  }


  def CalAvQE_sw(sc: SparkContext,
                synDataPath: String,
                commuteDataPath: String): Unit = {

    // 读取并处理synData数据（提取开始工作时间）
    val synData = sc.textFile(synDataPath)
      .map(_.split(","))
      .filter(_.length == 4)
      .map { parts =>
        val startTime = parts(1).split(" ")(0) // 提取开始工作时间（第二个字段的第一个单词）
        val workRegion = parts(1).split(" ")(1) // 提取工作区域（第二个字段的第二个单词）
        ((startTime,workRegion), 1)
      }
      .reduceByKey(_ + _) // 统计每个sw的出现次数

    // 读取并处理commuteData数据（提取开始时间）
    val commuteData = sc.textFile(commuteDataPath)
      .map(_.split(","))
      .filter(_.length == 4)
      .map { parts =>
        val startTime = parts(1).split(" ")(0) // 提取工作区域
        val workRegion = parts(1).split(" ")(1) // 提取工作区域（第二个字段的第二个单词）
        ((startTime,workRegion), 1)
      }
      .reduceByKey(_ + _) // 统计每个工作区域的出现次数

    // 全外连接，确保所有工作区域都被包含
    val joinedCounts = commuteData.fullOuterJoin(synData)
      .map { case ((startTime,workRegion), (comCountOpt, synCountOpt)) =>
        val comCount = comCountOpt.getOrElse(0)
        val synCount = synCountOpt.getOrElse(0)
        ((startTime,workRegion), comCount, synCount)
      }

    // 计算相对误差
    val errorRDD = joinedCounts.map { case ((startTime,workRegion), comCount, synCount) =>
      val denominator = scala.math.max(comCount.toDouble, 270)
      val relativeError = math.abs(synCount.toDouble - comCount.toDouble) / denominator
      ((startTime,workRegion), comCount, synCount, relativeError)
    }

    // 尝试采样500行，如果数据不足则使用全部数据
    val sampleSize = 500L
    val totalCount = errorRDD.count()
    val sampleFraction = if (totalCount > sampleSize) sampleSize.toDouble / totalCount else 1.0

    val sampledErrors = errorRDD.sample(withReplacement = false, fraction = sampleFraction)

    // 计算平均相对误差
    val avgError = if (sampledErrors.isEmpty()) {
      0.0
    } else {
      val (sum, count) = sampledErrors.map { case (_, _, _, error) =>
        (error, 1L)
      }.reduce { case ((sum1, count1), (sum2, count2)) =>
        (sum1 + sum2, count1 + count2)
      }
      sum / count
    }

    // 打印结果到控制台
    println("======================================")
    println(s"*****************Average sw Error: $avgError*******************")
    println("======================================")
  }

  def CalAvQE_fw(sc: SparkContext,
                synDataPath: String,
                commuteDataPath: String): Unit = {

    // 读取并处理synData数据（提取结束工作时间）
    val synData = sc.textFile(synDataPath)
      .map(_.split(","))
      .filter(_.length == 4)
      .map { parts =>
        val startTime = parts(1).split(" ")(0).toInt // 提取开始工作时间（第二个字段的第一个单词）
        val workDuration = parts(2) .toInt //工作时长
        val finishTime = startTime + workDuration //结束工作时间
        val workRegion = parts(1).split(" ")(1)
        ((finishTime,workRegion), 1)
      }
      .reduceByKey(_ + _) // 统计每个开始时间的出现次数

    // 读取并处理commuteData数据（提取开始时间）
    val commuteData = sc.textFile(commuteDataPath)
      .map(_.split(","))
      .filter(_.length == 4)
      .map { parts =>
        val startTime = parts(1).split(" ")(0).toInt // 提取工作区域
        val workDuration = parts(2).toInt
        val finishTime = startTime + workDuration
        val workRegion = parts(1).split(" ")(1)
        ((finishTime,workRegion), 1)
      }
      .reduceByKey(_ + _) // 统计每个工作区域的出现次数

    // 全外连接，确保所有工作区域都被包含
    val joinedCounts = commuteData.fullOuterJoin(synData)
      .map { case ((finishTime,workRegion), (comCountOpt, synCountOpt)) =>
        val comCount = comCountOpt.getOrElse(0)
        val synCount = synCountOpt.getOrElse(0)
        ((finishTime,workRegion), comCount, synCount)
      }

    // 计算相对误差
    val errorRDD = joinedCounts.map { case ((finishTime,workRegion), comCount, synCount) =>
      val denominator = scala.math.max(comCount.toDouble, 270)
      val relativeError = math.abs(synCount.toDouble - comCount.toDouble) / denominator
      ((finishTime,workRegion), comCount, synCount, relativeError)
    }

    // 尝试采样500行，如果数据不足则使用全部数据
    val sampleSize = 500L
    val totalCount = errorRDD.count()
    val sampleFraction = if (totalCount > sampleSize) sampleSize.toDouble / totalCount else 1.0

    val sampledErrors = errorRDD.sample(withReplacement = false, fraction = sampleFraction)

    // 计算平均相对误差
    val avgError = if (sampledErrors.isEmpty()) {
      0.0
    } else {
      val (sum, count) = sampledErrors.map { case (_, _, _, error) =>
        (error, 1L)
      }.reduce { case ((sum1, count1), (sum2, count2)) =>
        (sum1 + sum2, count1 + count2)
      }
      sum / count
    }

    // 打印结果到控制台
    println("======================================")
    println(s"***************Average fw Error: $avgError******************")
    println("======================================")
  }







  def AdaAvQE_h(sc: SparkContext,
                synDataPath: String,
                commuteDataPath: String): Unit = {

    // 读取并处理synData数据 - 使用完整第一个字段作为家标识
    val synData = sc.textFile(synDataPath)
      .map(_.split(","))
      .filter(_.length == 2)
      .map { parts =>
        val homeLocation = parts(0)  // 直接使用完整坐标字符串
        (homeLocation, 1)
      }
      .reduceByKey(_ + _)

    // 读取并处理commuteData数据 - 同样使用完整第一个字段
    val commuteData = sc.textFile(commuteDataPath)
      .map(_.split(","))
      .filter(_.length == 2)
      .map { parts =>
        val homeLocation = parts(0)  // 直接使用完整坐标字符串
        (homeLocation, 1)
      }
      .reduceByKey(_ + _)

    // 全外连接处理所有家坐标点
    val joinedCounts = commuteData.fullOuterJoin(synData)
      .map { case (homeLocation, (comCountOpt, synCountOpt)) =>
        val comCount = comCountOpt.getOrElse(0)
        val synCount = synCountOpt.getOrElse(0)
        (homeLocation, comCount, synCount)
      }

    // 计算相对误差（分母取max(comCount,750)）
    val errorRDD = joinedCounts.map { case (homeLocation, comCount, synCount) =>
      val denominator = math.max(comCount.toDouble, 750)
      val relativeError = math.abs(synCount - comCount) / denominator
      (homeLocation, comCount, synCount, relativeError)
    }

    // 采样500行或全量计算
    val sampleSize = 500L
    val totalCount = errorRDD.count()
    val sampleFraction = if (totalCount > sampleSize) sampleSize.toDouble / totalCount else 1.0
    val sampledErrors = errorRDD.sample(false, sampleFraction)

    // 计算平均相对误差
    val avgError = if (sampledErrors.isEmpty()) 0.0 else {
      sampledErrors.map(_._4).reduce(_ + _) / sampledErrors.count()
    }

    // 输出结果
    println("======================================")
    println(s"***** Average Relative Error Home Location: $avgError *****")
    println("======================================")
  }
  def AdaAvQE_w(sc: SparkContext,
                synDataPath: String,
                commuteDataPath: String): Unit = {

    // 读取并处理synData数据（提取工作区域）
    val synData = sc.textFile(synDataPath)
      .map(_.split(","))
      .filter(_.length == 2)
      .map { parts =>
        val workRegion = parts(1) // 提取工作区域（第二个字段的第二个单词）
        (workRegion, 1)
      }
      .reduceByKey(_ + _) // 统计每个工作区域的出现次数

    // 读取并处理commuteData数据（提取工作区域）
    val commuteData = sc.textFile(commuteDataPath)
      .map(_.split(","))
      .filter(_.length == 2)
      .map { parts =>
        val workRegion = parts(1) // 提取工作区域
        (workRegion, 1)
      }
      .reduceByKey(_ + _) // 统计每个工作区域的出现次数

    // 全外连接，确保所有工作区域都被包含
    val joinedCounts = commuteData.fullOuterJoin(synData)
      .map { case (workRegion, (comCountOpt, synCountOpt)) =>
        val comCount = comCountOpt.getOrElse(0)
        val synCount = synCountOpt.getOrElse(0)
        (workRegion, comCount, synCount)
      }

    // 计算相对误差
    val errorRDD = joinedCounts.map { case (workRegion, comCount, synCount) =>
      val denominator = scala.math.max(comCount.toDouble, 750)
      val relativeError = math.abs(synCount.toDouble - comCount.toDouble) / denominator
      (workRegion, comCount, synCount, relativeError)
    }

    // 尝试采样500行，如果数据不足则使用全部数据
    val sampleSize = 500L
    val totalCount = errorRDD.count()
    val sampleFraction = if (totalCount > sampleSize) sampleSize.toDouble / totalCount else 1.0

    val sampledErrors = errorRDD.sample(withReplacement = false, fraction = sampleFraction)

    // 计算平均相对误差
    val avgError = if (sampledErrors.isEmpty()) {
      0.0
    } else {
      val (sum, count) = sampledErrors.map { case (_, _, _, error) =>
        (error, 1L)
      }.reduce { case ((sum1, count1), (sum2, count2)) =>
        (sum1 + sum2, count1 + count2)
      }
      sum / count
    }

    // 打印结果到控制台
    println("======================================")
    println(s"*********************Average Work Region Error: $avgError*************")
    println("======================================")
  }

}