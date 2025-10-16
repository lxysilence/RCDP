
//提取区域id，站点，经度，纬度作为映射表

package RCDPjour.evaluation

import common.CommonScalaMethod
import utils.SystemUtils

object SationRegionInfo {

    //北斗集群中数据的路径
    //由于BfmapReader读取地图文件使用的是new FileInputStream方法， 故只能使用yarn-client模式运行程序，且文件路径需绝对路径
    //如果要用yarn-cluster模式，就只能手动重写BfmapReader类再打包安装依赖
    var mapPath = "/root/lxying/map/overpass.bfmap"
    var dataPathPrefix = "/user/root/lxying/data/2018/"
    var resultPathPrefix = "/user/root/lxying/resultData/pro/"
    var regionDataPath = "/user/root/lxying/sz_points_v3.csv"


    //中心集群中数据的路径
    //    val mapPath = "/home/frzheng/lxying_files/projects/map/map.bfmap"
    //    val dataPathPrefix = "/user/lxying/testData/taxiGps/"
    //    var resultPathPrefix = "/user/lxying/resultData/GetMaxErrorRoad/"


    //本地数据路径
    if (SystemUtils.isWindows) {
      dataPathPrefix = "D:\\experiment\\RCDPjour\\commuteData\\commute_line_filter"
      resultPathPrefix = "D:\\experiment\\RCDPjour\\index"
      regionDataPath = "output/sz_points_v3.csv"
    }

    def main(args: Array[String]): Unit = {

      val sc = CommonScalaMethod.buildSparkContext("pro_APP")

      val stationData = sc.textFile(dataPathPrefix + "/part*")
        .flatMap { line =>
          val fields = line.split(",", -1) // 保留空字段

          if (fields.length >= 9) {
            // 提取起始位置信息
            val startStation = (fields(1), fields(2), fields(3), fields(4))

            // 提取结束位置信息
            val endStation = (fields(5), fields(6), fields(7), fields(8))

            Seq(startStation, endStation)
          } else {
            // 跳过无效行
            Seq.empty
          }
        }
        .distinct() // 去重
        .map { case (regionId, station, lng, lat) =>
          s"$regionId,$station,$lng,$lat" // 格式化为输出字符串
        }



      // 定义输出文件路径
      stationData.coalesce(1).saveAsTextFile(resultPathPrefix + "/Region")

      sc.stop()

    }
}
