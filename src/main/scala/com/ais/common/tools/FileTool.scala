package com.ais.common.tools

import java.io.{File, FileInputStream}
import java.util.Properties

/** Created by ShiGZ on 2016/9/2. */
object FileTool {

  /** 从给定路径下读取配置信息
    *
    * @param path 路径
    * @return 配置信息Properties
    */
  def propertiesFromPath(path: String): Properties = {
    val result = new Properties()
    val inputStream = new FileInputStream(path)
    result.load(inputStream)
    inputStream.close()
    result
  }

  /** 判断是否存在该文件
    *
    * @param path 文件路径
    * @return 是否存在
    */
  def isFileExist(path: String): Boolean = {
    val file = new File(path)
    file.exists()
  }

  /** 将给定路径的文件进行删除
    *
    * @param path 文件路径
    * @return 是否删除成功
    */
  def deleteFile(path: String): Boolean = {
    val file = new File(path)
    if (isFileExist(path)) {
      file.delete()
    } else {
      true
    }
  }

}
