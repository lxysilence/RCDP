package RCDPjour.others

import org.apache.spark.{SparkConf, SparkContext}

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ArrayBuffer

object stdFloat2int {
  def main(args: Array[String]): Unit = {

    val start_time: Long = new Date().getTime
    val conf: SparkConf = new SparkConf()
      .setAppName("stdFloat2int")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)


    val dataPath: String = "C:\\Users\\syc\\Desktop\\resultData\\StatisticMatchError\\2017-01-01__2017-01-07\\*\\part-*"
    val savePath = "D:\\IdeaProject\\TrafficCompletionProject\\lxying_files\\resultData\\roadTest\\2017bigResult"
    //    val file = new File(dataPath)
    //    val iterator: Iterator[File] = file.listFiles().iterator
    //    var DataNum = 0
    //
    //    val DateBuf = new ArrayBuffer[String]
    //    while (iterator.hasNext) {
    //      val dataPath: String = iterator.next().toString
    //      val strings: Array[String] = dataPath.split('\\')
    //      val dataName: String = strings(strings.length - 1)
    //      if (dataName.contains("test")) {
    //        DataNum += 1
    //        DateBuf.append(dataPath + "/part-*")
    //      }
    //    }

    val TopNRDD = sc.textFile(dataPath).map(line => {
      //      val fields: Array[String] = line.split(",")
      val error = line.toFloat.toInt
      (error, 1)
    })

      .reduceByKey(_ + _) //不分时间的累加
      //      .map(line =>{
      ////      (line._1._1,line._1._2,line._1._3,line._1._4,line._2.toFloat/DataNum) //没必要球均值
      //      (line._1._1,line._1._2,line._1._3,line._1._4,line._2)
      //    })
      //      .sortBy(_._5,ascending = false)// 降序
      //      .map(line =>{//修改key
      //        ((line._1._2,line._1._4),line._1._1,line._1._3,line._2)
      //      })
      //      .groupBy(_._1)
      //      .map(line =>{
      //        val ot: Int = line._1._1
      //        val dt: Int = line._1._2
      //        val datas: Array[((Int, Int), Int, Int, Long)] = line._2.toArray
      //
      //        var sum = 0L
      //        for (i <- datas.indices) {
      //          sum += datas(i)._4
      //        }
      //        (ot,dt,sum)
      //    })
      //      .sortBy(_._3,ascending = false)// 降序

      .sortBy(_._1, ascending = true) // sheng序
      //      .take(20)
      .map(line => {
        val tmp = new ArrayBuffer[String]()
        for (i <- line.productIterator) {
          tmp.append(i.toString)
        }
        tmp.mkString(",")
      })

    val bool: Boolean = deletePath(savePath)
    if (bool) {
      println("文件删除成功")
    } else {
      println("文件不存在")
    }

    TopNRDD.repartition(1)
      .saveAsTextFile(savePath)

    sc.stop()
    val end_time: Long = new Date().getTime
    val nowDate = NowDate()
    println(nowDate + " costtime = " + (end_time - start_time) / 1000.0) //单位毫秒
  }

  def deleteFilePath(dirFile: File): Boolean = {
    // 如果dir对应的文件不存在，则退出
    if (!dirFile.exists()) return false

    if (dirFile.isFile()) {
      return dirFile.delete();
    } else {

      for (file <- dirFile.listFiles()) {
        deleteFilePath(file);
      }
    }
    dirFile.delete();
  }

  def deletePath(dirFile: String): Boolean = {
    val file1 = new File(dirFile)
    deleteFilePath(file1)
  }

  def NowDate(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = dateFormat.format(now)
    return date
  }
}
