package common

import org.apache.spark.{SparkConf, SparkContext}
import utils.SystemUtils

object CommonScalaMethod {

  /**
   * 本地Windows环境下setMaster("local[*]")，否则不设置master
   */
  def buildSparkContext(appName: String): SparkContext = {
    val sparkConf = new SparkConf()
      .setAppName(appName)

    if (SystemUtils.isWindows) {
      sparkConf.setMaster("local[*]")
    }
    new SparkContext(sparkConf)
  }
}
