package com.ais.common.tools

import java.net.URI

import com.ais.common.LoggerObject
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/** Created by ShiGZ on 2016/8/10. */
object HadoopTool extends LoggerObject {

  def initHadoopConfig(): Configuration = {
    val result = new Configuration()
    result.set("dfs.client.failover.proxy.provider." + "ns",
      "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
    result.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")
    result.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true")
    //防止 file System io closed
    result.setBoolean("fs.hdfs.impl.disable.cache", true)
    result
  }

  /** 根据路径取出目标路径下所有的file status
    *
    * @param dirPath 目录路径
    * @return
    */
  def getFileStatusesFromPath(dirPath: String): Array[FileStatus] = {
    val fileSystem = FileSystem.newInstance(URI.create(dirPath), initHadoopConfig())
    try {
      val filePath = new Path(dirPath)
      if (fileSystem.exists(filePath)) {
        fileSystem.listStatus(filePath)
      } else {
        new Array[FileStatus](0)
      }
    } finally {
      fileSystem.close()
    }

  }

  /** 根据路径取出第一个文件的名称
    *
    * @param dirPath 目录路径
    * @return
    */
  def getFirstFileNameOptionFromPath(dirPath: String): Option[String] = {
    val radiusFileNameLB = getFileNameABFromPath(dirPath)
    if (0 < radiusFileNameLB.size) {
      Some(radiusFileNameLB.head)
    } else {
      None
    }
  }

  def getFirstValidFileNameOption(dirPath: String, interval: Long): Option[String] = {
    for (radiusLogStatus <- getFileStatusesFromPath(dirPath)) {
      val fileName = radiusLogStatus.getPath.getName
      if (System.currentTimeMillis() - fileName.toLong >= interval) {
        return Some(fileName)
      }
    }
    None
  }

  def getFileNameABFromPathOfMaxCount(dirPath: String, maxCount: Int): ArrayBuffer[String] = {
    val result = new ArrayBuffer[String]()
    for (radiusLogStatus <- getFileStatusesFromPath(dirPath)) {
      val fileName = radiusLogStatus.getPath.getName
      if (!fileName.endsWith(".TMP")) {
        result.append(fileName)
        if (result.size >= maxCount) {
          return result
        }
      }
    }
    result
  }

  def getFileNameABFromPathOfMaxCount(dirPath: String, maxCount: Int, fileNameSeparator: String, maxMillis: Long): ArrayBuffer[String] = {
    val result = new ArrayBuffer[String]()
    for (radiusLogStatus <- getFileStatusesFromPath(dirPath)) {
      val fileName = radiusLogStatus.getPath.getName
      if (!fileName.endsWith(".TMP")) {
        try {
          if (ArrayTool.getTextArrWithSeparator(fileName, fileNameSeparator).last.toLong < maxMillis) {
            result.append(fileName)
            if (result.size >= maxCount) {
              return result
            }
          }
        } catch {
          case ex: Exception => errorLog("判断输入文件名 ", fileName, " 是否符合延迟时间条件时出现了异常！异常信息为:", ex.getCause)
        }
      }
    }
    result
  }

  def getFileNameListAndPeriodsFromPathOfMaxCount(dirPath: String, maxCount: Int, fileNameSeparator: String, maxMillis: Long): (Long, ListBuffer[String]) = {
    val fileNameList = new ListBuffer[String]()
    val tempMap = new mutable.HashMap[Long, String]()
    for (radiusLogStatus <- getFileStatusesFromPath(dirPath)) {
      val fileName = radiusLogStatus.getPath.getName
      if (!fileName.endsWith(".TMP")) {
        try {
          val collectMillis = ArrayTool.getTextArrWithSeparator(fileName, fileNameSeparator).last.toLong
          if (collectMillis < maxMillis) {
            tempMap.put(collectMillis, fileName)
          }
        } catch {
          case ex: Exception => errorLog("判断输入文件名 ", fileName, " 是否符合延迟时间条件时出现了异常！异常信息为:", ex.getCause)
        }
      }
    }
    if (tempMap.size >= maxCount) {
      val keyList = tempMap.keySet.toList.sortWith(_ < _).take(maxCount)
      for (collectMillis <- keyList) {
        fileNameList.append(tempMap.getOrElse(collectMillis, ""))
      }
      (keyList.last, fileNameList)
    } else {
      fileNameList.appendAll(tempMap.valuesIterator)
      (maxMillis, fileNameList)
    }
  }

  /** 根据路径取出目标路径下所有的文件名
    *
    * @param dirPath 路径
    * @return
    */
  def getFileNameABFromPath(dirPath: String): ArrayBuffer[String] = {
    val result = new ArrayBuffer[String]()
    for (radiusLogStatus <- getFileStatusesFromPath(dirPath)) {
      val fileName = radiusLogStatus.getPath.getName
      if (!fileName.endsWith(".TMP")) {
        result.append(fileName)
      }
    }
    result
  }

  def getFileNameABFromPath(dirPath: String, earliestTime: Long,latestTime:Long): ArrayBuffer[String] = {
    val result = new ArrayBuffer[String]()
    for (radiusLogStatus <- getFileStatusesFromPath(dirPath)) {
      val fileName = radiusLogStatus.getPath.getName
      try {

        if (!fileName.endsWith(".TMP")) {
          val fileTime = fileName.toLong
          if (earliestTime <= fileTime && fileTime <= latestTime) {
            result.append(fileName)
          }
        }
      } catch {
        case ex: Exception => errorLog("判断输入文件名 ", fileName, " 是否符合延迟时间条件时出现了异常！异常信息为:", ex.getCause)
      }
    }
    result
  }

  /** 在指定目录下查找目标文件,并删除
    *
    * @param dirPath  目录路径
    * @param fileName 文件名
    * @return
    */
  def deleteFile(dirPath: String, fileName: String): Unit = {
    val fileSystem = FileSystem.newInstance(URI.create(dirPath), initHadoopConfig())
    try {
      val filePath = new Path(StringTool.joinValues(dirPath, "/", fileName))
      if (fileSystem.exists(filePath)) {
        fileSystem.delete(filePath, true)
      }
    } finally {
      fileSystem.close()
    }
  }

  /** 在指定目录下寻找指定文件,并将它改名
    *
    * @param dirPath     目录路径
    * @param fileName    旧文件名
    * @param newFilePath 新文件路径
    * @return
    */
  def renameFile(dirPath: String, fileName: String, newFilePath: String): Unit = {
    val fileSystem = FileSystem.newInstance(URI.create(dirPath), initHadoopConfig())
    try {
      val filePath = new Path(StringTool.joinValues(dirPath, "/", fileName))
      val destPath = new Path(newFilePath)
      if (fileSystem.exists(filePath)) {
        fileSystem.rename(filePath, destPath)
      }
    } finally {
      fileSystem.close()
    }

  }

  def mkDir(parentDirPath: String, dirPath: String): Unit = {
    val fileSystem = FileSystem.newInstance(URI.create(parentDirPath), initHadoopConfig())
    try {
      val filePath = new Path(dirPath)
      if (!fileSystem.exists(filePath)) {
        fileSystem.mkdirs(filePath)
      }
    } finally {
      fileSystem.close()
    }
  }

}

