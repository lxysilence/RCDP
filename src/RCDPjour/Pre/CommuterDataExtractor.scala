package RCDPjour.Pre

import common.CommonScalaMethod
import utils.SystemUtils

object CommuterDataExtractor {

  //北斗集群中数据的路径
  //由于BfmapReader读取地图文件使用的是new FileInputStream方法， 故只能使用yarn-client模式运行程序，且文件路径需绝对路径
  //如果要用yarn-cluster模式，就只能手动重写BfmapReader类再打包安装依赖
  var mapPath = "/root/lxying/map/overpass.bfmap"
  var rawDataPathPrefix = "/user/lxying/RCDPjour/rawData"
  var commuteDataPrefix = "/user/lxying/RCDPjour/commuteData"
  var regionDataPath = "/user/lxying/RCDPjour/sz_points_v3.csv"


  ////  中心集群中数据的路径
  //      val mapPath = "/home/frzheng/lxying_files/projects/map/map.bfmap"
  //      val dataPathPrefix = "/user/lxying/testData/taxiGps/"
  //      var resultPathPrefix = "/user/lxying/resultData/GetMaxErrorRoad/"


  //本地数据路径
  if (SystemUtils.isWindows) {
    rawDataPathPrefix = "D:\\experiment\\RCDPjour\\rawData"
    commuteDataPrefix = "D:\\experiment\\RCDPjour\\commuteData"
    regionDataPath = "output/sz_points_v3.csv"
  }

  def main(args: Array[String]): Unit = {

    val sc = CommonScalaMethod.buildSparkContext("pro_APP")

    try{

////      Step1,从原始trip数据中筛选出有家和工作地的行，并添加时间戳和区域编号
//      HomeWorkCommuteTagger.tagCommuteTrips(
//        sc,
//        s"$rawDataPathPrefix/*/*",       // 原始trip数据路径
//        s"$regionDataPath",
//        s"$commuteDataPrefix/trip_tagCommute"          //
//      )
//
//      println("**********已经完成Step1的运行************")
////      Step2,将Step1中筛选出的数据，同一用户一天的不同行程拼接
////      数据格式：id,日期，家区域，站点，经度，纬度，工作区域，站点，经度，纬度，出发时间戳，出发时间，到达时间戳，到达时间，标记，出发时间戳，出发时间，到达时间戳，到达时间，标记...
//      DailyCommuteODMerger.DailyCommuteODMerge(
//        sc,
//        s"$commuteDataPrefix/trip_tagCommute/*",       // 原始trip数据路径
//        s"$commuteDataPrefix/commute_line"          //
//      )
//      println("**********已经完成Step2的运行************")

////      Step3，从第二步得到的数据，筛选出往返通勤数据，去掉日期，同一id只保留一条记录
////      数据格式：id,日期，家区域，站点，经度，纬度，工作区域，站点，经度，纬度，出发时间戳，出发时间，到达时间戳，到达时间，标记，出发时间戳，出发时间，到达时间戳，到达时间，标记
//      //这里的时间戳是10min为间隔划分的
//      DailyCommuteODMerger_filter.DailyCommutefilter(
//        sc,
//        s"$commuteDataPrefix/commute_line/*",       // 原始trip数据路径
//        s"$commuteDataPrefix/commute_line_filter"          //
//      )


//      //Step4,从第三步得到的数据，提取出RCDP需要的数据格式
//      //数据格式：出发时间戳 家区域，到达时间戳 工作区域，工作时长，往返通勤差异
//      DailyCommuteFilter_RCDP_15min.CalDailyCommute(
//        sc,
//        s"$commuteDataPrefix/commute_line_filter/*",
//        s"$commuteDataPrefix/commute_RCDP_10min"
//      )
////
////      Step5,从第四步得到的数据，提取出RCDP的时间自适应离散化版本
////      数据格式：出发时间戳 家区域，到达时间戳 工作区域，工作时长，往返通勤差异
////      计算区域客流量：区域，时间戳，客流量计数
//      RCDPAdaptive.CalRegionFlow(
//        sc,
//        s"$commuteDataPrefix/commute_RCDP/*",
////        s"$commuteDataPrefix/commute_RCDP_adaptive"
//        s"$commuteDataPrefix/RegionFlow"
//      )
//      RCDPAdaptive.AdaptiveTimeDiscretizer(
//        sc,
//        s"$commuteDataPrefix/commute_Chen_10min/part-00000",
//        s"$commuteDataPrefix/RegionFlow/index.txt",
//        s"$commuteDataPrefix/commute_RCDP_Station"
//      )

//      RCDPAblation.GridLevel(
//        sc,
//        s"$commuteDataPrefix/commute_line_filter/*",
//        s"$commuteDataPrefix/MoveSim/index/gps",
//        s"$commuteDataPrefix/commute_RCDP_Grid",
//      )


////    转化为Chen的建树方法所需数据格式
//      DailyCommuteFilter_Chen_15min.CalDailyCommute(
//        sc,
//        s"$commuteDataPrefix/commute_line_filter/*",
//        s"$commuteDataPrefix/commute_Chen_10min"
//      )




      //    转化为MoveSim方法所需数据格式
//      //Step1生成地址映射表，经纬度保留三位有效数字，排成一列，经纬度所在行数为其对应地址编码
//      GeoMappingGenerator.GenerateGeoMap(
//        sc,
//        s"$commuteDataPrefix/commute_line_filter/*",
//        s"$commuteDataPrefix/MoveSim/index"
//      )

      //Step2  转化为MoveSim所需数据格式
      //其中resultData数据格式为：去程出发时间戳 家位置，去程到达时间戳 工作位置，回程出发时间戳 工作位置，回程到达时间戳 家位置
      //finalData为movesim运行的实际格式
//      DailyCommuteFilter_MoveSim.CalDailyCommute(
//        sc,
//        s"$commuteDataPrefix/commute_line_filter/*",
//        s"$commuteDataPrefix/MoveSim/index/gps",
//        s"$commuteDataPrefix/MoveSim/30min",
//        s"$commuteDataPrefix/MoveSim/movesim"
//      )
//      //从movesim中提取出realdata,testdata,valdata,dispredata
//      MoveSimExtractor.ExtractDailyCommute(
//        sc,
//        s"$commuteDataPrefix/commute_RCDP_adaptive/75/part-00000",
//        s"$commuteDataPrefix/commute_RCDP_adaptive/50",
//        s"$commuteDataPrefix/MoveSim/real",
//        s"$commuteDataPrefix/MoveSim/test",
//        s"$commuteDataPrefix/MoveSim/val"
//      )
      //Movsim输出格式转化为可评估格式
      DailyCommuteFilter_MoveSim.Change(
        sc,
        s"$commuteDataPrefix/MoveSim/gene.data",
        s"$commuteDataPrefix/MoveSim/gene"
      )




//      //转化为Adatrace所需数据格式
//      AdatraceExtractor.ExtractDailyCommute(
//        sc,
//        s"$commuteDataPrefix/commute_line_filter/*",
//        s"$commuteDataPrefix/Adatrace"
//      )



      println("**********已经完成所有程序的运行************")
    }finally {
      sc.stop()
    }
  }

}
