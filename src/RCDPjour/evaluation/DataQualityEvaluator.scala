package RCDPjour.evaluation
import common.CommonScalaMethod
import utils.SystemUtils
object DataQualityEvaluator {

  //北斗集群中数据的路径
  //由于BfmapReader读取地图文件使用的是new FileInputStream方法， 故只能使用yarn-client模式运行程序，且文件路径需绝对路径
  //如果要用yarn-cluster模式，就只能手动重写BfmapReader类再打包安装依赖
  var mapPath = "/root/lxying/map/overpass.bfmap"
  var synDataPathPrefix = "/user/lxying/RCDPjour/genData"
  var commuteDataPrefix = "/user/lxying/RCDPjour/commuteData"
  var evaluaDataPrefix = "/user/lxying/RCDPjour/evaluation"
  var regionDataPath = "/user/lxying/RCDPjour/sz_points_v3.csv"


  ////  中心集群中数据的路径
  //      val mapPath = "/home/frzheng/lxying_files/projects/map/map.bfmap"
  //      val dataPathPrefix = "/user/lxying/testData/taxiGps/"
  //      var resultPathPrefix = "/user/lxying/resultData/GetMaxErrorRoad/"


  //本地数据路径
  if (SystemUtils.isWindows) {
    synDataPathPrefix = "D:\\experiment\\RCDPjour\\genData\\RCDP\\200\\8.txt"
    commuteDataPrefix = "D:\\experiment\\RCDPjour\\commuteData\\commute_RCDP_adaptive\\200\\part*"
    evaluaDataPrefix = "D:\\experiment\\RCDPjour\\evaluation"
    regionDataPath = "output/sz_points_v3.csv"
  }


//  if (SystemUtils.isWindows) {
//    synDataPathPrefix = "D:\\experiment\\RCDPjour\\commuteData\\Adatrace\\20\\part*"
//    commuteDataPrefix = "D:\\experiment\\RCDPjour\\commuteData\\Adatrace\\99\\part*"
//    evaluaDataPrefix = "D:\\experiment\\RCDPjour\\evaluation"
//    regionDataPath = "output/sz_points_v3.csv"
//  }
  def main(args: Array[String]): Unit = {

    val sc = CommonScalaMethod.buildSparkContext("pro_APP")

    try{

//      //Adatrace转化为评估格式
//      Change.Adatrace(
//        sc,
//        s"$commuteDataPrefix",       // 原始trip数据路径
//        s"$synDataPathPrefix",
//      )
//      AvQE.AdaAvQE_h(
//        sc,
//        s"$synDataPathPrefix",       // 原始trip数据路径
//        s"$commuteDataPrefix",
//      )
//      AvQE.AdaAvQE_w(
//        sc,
//        s"$synDataPathPrefix",       // 原始trip数据路径
//        s"$commuteDataPrefix",
//      )
//      KT.AdaKT_h(
//        sc,
//        s"$synDataPathPrefix",       // 原始trip数据路径
//        s"$commuteDataPrefix",
//      )
//      KT.AdaKT_w(
//        sc,
//        s"$synDataPathPrefix",       // 原始trip数据路径
//        s"$commuteDataPrefix",
//      )
//      JSD.CalJSD_hw(
//        sc,
//        s"$synDataPathPrefix",       // 原始trip数据路径
//        s"$commuteDataPrefix",
//      )






      //空间相关评估
      //AvQE_h
      AvQE.CalAvQE_h(
        sc,
        s"$synDataPathPrefix",       // 原始trip数据路径
        s"$commuteDataPrefix",
      )

      //AvQE_w
      AvQE.CalAvQE_w(
        sc,
        s"$synDataPathPrefix",       // 原始trip数据路径
        s"$commuteDataPrefix",
      )

      //KT_h
      KT.CalKT_h(
        sc,
        s"$synDataPathPrefix",       // 原始trip数据路径
        s"$commuteDataPrefix",
      )

      //KT_w
      KT.CalKT_w(
        sc,
        s"$synDataPathPrefix",       // 原始trip数据路径
        s"$commuteDataPrefix",
      )

      //JSD_hw
      JSD.CalJSD_hw(
        sc,
        s"$synDataPathPrefix",       // 原始trip数据路径
        s"$commuteDataPrefix",
      )


      //时间相关评估
      //AvQE_s
      AvQE.CalAvQE_s(
        sc,
        s"$synDataPathPrefix",       // 原始trip数据路径
        s"$commuteDataPrefix",
      )

      //AvQE_f
      AvQE.CalAvQE_f(
        sc,
        s"$synDataPathPrefix",       // 原始trip数据路径
        s"$commuteDataPrefix",
      )

      //JSD_hw
      JSD.CalJSD_c(
        sc,
        s"$synDataPathPrefix",       // 原始trip数据路径
        s"$commuteDataPrefix",
      )

      //JSD_hw
      JSD.CalJSD_d(
        sc,
        s"$synDataPathPrefix",       // 原始trip数据路径
        s"$commuteDataPrefix",
      )



      //时空相关评估
      //AvQE_sw
      AvQE.CalAvQE_sw(
        sc,
        s"$synDataPathPrefix",       // 原始trip数据路径
        s"$commuteDataPrefix",
      )

      //AvQE_fw
      AvQE.CalAvQE_fw(
        sc,
        s"$synDataPathPrefix",       // 原始trip数据路径
        s"$commuteDataPrefix",
      )

      //JSD_chw
      JSD.CalJSD_chw(
        sc,
        s"$synDataPathPrefix",       // 原始trip数据路径
        s"$commuteDataPrefix",
      )

      //JSD_dw
      JSD.CalJSD_dw(
        sc,
        s"$synDataPathPrefix",       // 原始trip数据路径
        s"$commuteDataPrefix",
      )

      println("**********已经完成所有程序的运行************")
    }finally {
      sc.stop()
    }
  }


}
