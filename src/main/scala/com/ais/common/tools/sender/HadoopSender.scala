package com.ais.common.tools.sender

import com.ais.common.LoggerObject
import org.apache.spark.rdd.RDD

/** Created by ShiGZ on 2017/2/21. */
class HadoopSender extends LoggerObject {

  def send(data: RDD[String]): Unit = {
  }

  def send(data: RDD[String], count: Int): Unit = {
  }

}


class ExtendHadoopSender(dirPath: String) extends HadoopSender {

  override def send(data: RDD[String]): Unit = {
    data.saveAsTextFile(dirPath + "/" + System.currentTimeMillis())
    infoLog("写数据到 ", dirPath, " 成功！")
  }

  override def send(data: RDD[String], count: Int): Unit = {
    data.saveAsTextFile(dirPath + "/" + count + "/" + System.currentTimeMillis())
    infoLog("写数据到 ", dirPath, "/ ", count, "成功！")
  }

}

object HadoopSender {

  def getInstance(dirPath: String): HadoopSender = {
    new ExtendHadoopSender(dirPath)
  }

  def getInstance(dirPathOption: Option[String]): HadoopSender = {
    if (dirPathOption.isDefined) {
      new ExtendHadoopSender(dirPathOption.get)
    } else {
      new HadoopSender()
    }
  }

}
