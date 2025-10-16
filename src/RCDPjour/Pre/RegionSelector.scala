package RCDPjour.Pre

import experiment.others.GeneralFunctionSets.PolygonNum
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object RegionSelector {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("RegionSelector_Test")
      .setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val selector = new RegionSelector()
    selector.initialize(sc, "output/sz_points_v3.csv")
    val regionID: Int = selector.queryRegion(114.136624,22.576996)
    println(regionID)
  }
}

class   RegionSelector extends Serializable {

  var mPoints = new ArrayBuffer[ArrayBuffer[ArrayBuffer[Double]]]()
  var minMaxRegion: Array[Array[Array[Double]]] = Array()
  var Centers: Array[Array[Double]] = Array()

  def initialize(sc: SparkContext, regionDataPath: String): Unit = {
    val szRegionData: Array[Array[Double]] = sc.textFile(regionDataPath)
      .map(line => {
        val strings: Array[String] = line.split(",")
        val strArrayBuf = new ArrayBuffer[Double]
        for(i <- 0 until strings.length) {
          strArrayBuf.append(strings(i).toDouble)
        }
        strArrayBuf.toArray
      })
      .collect()
      .sortBy(item => item(0))// scala按照区域排序,防止分布式后乱序


    val N: Int = szRegionData.length
    val row = 2
    val column = 2
    //(N,2,2);区域数量,最小经纬度,最大经纬度

    minMaxRegion = Array.ofDim[Double](N, row, column)
    Centers = Array.ofDim[Double](N, column)


    val dataStartIndex = 7

    for(i <- szRegionData.indices) {
      Centers(i)(0) = szRegionData(i)(1)// 中心经度
      Centers(i)(1) = szRegionData(i)(2)// 中心纬度
      minMaxRegion(i)(0)(0) = szRegionData(i)(3)// 最小经度
      minMaxRegion(i)(0)(1) = szRegionData(i)(4)// 最小纬度
      minMaxRegion(i)(1)(0) = szRegionData(i)(5)// 最大经度
      minMaxRegion(i)(1)(1) = szRegionData(i)(6)// 最大纬度
      val tmp1 = new ArrayBuffer[ArrayBuffer[Double]]()
      for (j <- 0 until (szRegionData(i).length - dataStartIndex) / 2) {
        val point = new ArrayBuffer[Double]
        point.append(szRegionData(i)(j*2 + dataStartIndex))
        point.append(szRegionData(i)(j*2 + 1 + dataStartIndex))
        tmp1.append(point)
      }
      mPoints.append(tmp1)
    }
  }

  /**
   *
   * @param longitude 经度
   * @param latitude 纬度
   * @return 区域编号
   */
  def queryRegion(longitude: Double, latitude: Double): Int = {
    val Point = new ArrayBuffer[Double]()
    Point.append(longitude)
    Point.append(latitude)
    PolygonNum(mPoints,minMaxRegion,Point,Centers)
  }
}
