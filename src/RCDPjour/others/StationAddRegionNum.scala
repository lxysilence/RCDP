package RCDPjour.others

import experiment.others.GeneralFunctionSets.PolygonNum
import experiment.others.stdFloat2int.deletePath
import org.apache.spark.{SparkConf, SparkContext}

import java.util.Date
import scala.collection.mutable.ArrayBuffer

object StationAddRegionNum {
  def main(args: Array[String]): Unit = {

    val start_time = new Date().getTime
    val conf = new SparkConf().setAppName("StationAddRegionNum").setMaster("local[*]")
    val sc = new SparkContext(conf)


    //    val StationDataPath = "D:\\subwayData\\mutiOD\\stations"
    val StationDataPath = "D:\\subwayData\\mutiOD\\subawayStaionData"
    //    val outPutPath = "D:\\subwayData\\mutiOD\\stationsOutputGCJ02V5"
    val outPutPath = "D:\\subwayData\\mutiOD\\subawayStaionOutputGCJ02V1"
    val szRegionDataPath = "D:\\subwayData\\mutiOD\\szRegionData"


    //val szRegionDataPath = "hdfs://compute-5-2:8020/user/zhaojuanjuan/zfr_files/szRegionData"
    //    val szRegionDataPath: String = args(4)

    //    val szRegionData: Array[Array[Double]] = sc.textFile(szRegionDataPath + "/sz_points_GCJ02_v4.csv")
    val szRegionData: Array[Array[Double]] = sc.textFile(szRegionDataPath + "testData/sz_points_v3.csv")
      .map(line => {
        val strings: Array[String] = line.split(",")
        val strArrayBuf = new ArrayBuffer[Double]
        for (i <- 0 until strings.length) {
          strArrayBuf.append(strings(i).toDouble)
        }
        strArrayBuf.toArray
      })
      .collect()
      .sortBy(item => item(0)) // scala按照区域排序,防止分布式后乱序


    val N: Int = szRegionData.length
    val row = 2
    val column = 2
    //(N,2,2);区域数量,最小经纬度,最大经纬度
    val minMaxRegion: Array[Array[Array[Double]]] = Array.ofDim[Double](N, row, column)

    val Centers: Array[Array[Double]] = Array.ofDim[Double](N, column)

    val mPoints = new ArrayBuffer[ArrayBuffer[ArrayBuffer[Double]]]()
    val dataStartIndex = 7

    for (i <- szRegionData.indices) {
      Centers(i)(0) = szRegionData(i)(1) // 中心经度
      Centers(i)(1) = szRegionData(i)(2) // 中心纬度
      minMaxRegion(i)(0)(0) = szRegionData(i)(3) // 最小经度
      minMaxRegion(i)(0)(1) = szRegionData(i)(4) // 最小纬度
      minMaxRegion(i)(1)(0) = szRegionData(i)(5) // 最大经度
      minMaxRegion(i)(1)(1) = szRegionData(i)(6) // 最大纬度
      val tmp1 = new ArrayBuffer[ArrayBuffer[Double]]()
      for (j <- 0 until (szRegionData(i).length - dataStartIndex) / 2) {
        val point = new ArrayBuffer[Double]
        point.append(szRegionData(i)(j * 2 + dataStartIndex))
        point.append(szRegionData(i)(j * 2 + 1 + dataStartIndex))
        tmp1.append(point)
      }
      mPoints.append(tmp1)
    }

    val outPut = sc.textFile(StationDataPath + "/part-r*")
      //      .map(line => {
      //      val outPut = sc.textFile("D:\\subwayData\\mutiOD\\Station1819.csv")
      .filter(line => {
        val fields: Array[String] = line.split(',')
        fields.length >= 1
      })
      .map(line => {
        val fields: Array[String] = line.split(',')

        val lat: Double = fields(5).toDouble
        val lon: Double = fields(4).toDouble
        val Point = new ArrayBuffer[Double]()
        Point.append(lat)
        Point.append(lon)


        val oRegionNum: Int = PolygonNum(mPoints, minMaxRegion, Point, Centers) //(单位MS)


        //      (fields(0),fields(1),fields(2),oRegionNum,Centers(oRegionNum)(0),Centers(oRegionNum)(1)) // 生成key value
        //        (fields(0),fields(1),oRegionNum,Centers(oRegionNum)(0),Centers(oRegionNum)(1)) // 生成key value
        (fields(0), fields(1), fields(2), fields(3), fields(4), fields(5), oRegionNum, Centers(oRegionNum)(0), Centers(oRegionNum)(1)) // 生成key value

      })
      .map(line => {
        val tmp = new ArrayBuffer[String]()
        for (i <- line.productIterator) {
          tmp.append(i.toString)
        }
        tmp.mkString(",")
      })


    val bool: Boolean = deletePath(outPutPath)
    if (bool) {
      println("文件删除成功")
    } else {
      println("文件不存在")
    }
    outPut.repartition(1).saveAsTextFile(outPutPath)

    sc.stop()
    val end_time: Long = new Date().getTime
    println("costtime = " + (end_time - start_time) / 1000.0) //单位毫秒
  }
}
