package utils

import org.apache.spark.{SparkConf, SparkContext}

import java.io.{File, PrintWriter}

/**
 * 从一个gz文件中读取前若干行并生成一个文件
 */
object ReadGzFile {

  val gzFilePath = "testData/busGpsData/part*"; //gz文件路径
  val num = 20000; //要读取的行数
  val outputPath = "testData/busGpsData/" + num + "row-testGpsData.txt"; //输出文件路径

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("ReadGzFile")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.textFile(gzFilePath)
    val outputWriter = new PrintWriter(new File(outputPath))
    rdd.take(num)
      .foreach(line => {
        outputWriter.println(line)
      })
    outputWriter.close()
  }
}
