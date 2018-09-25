package com.ais.common.tools

import java.util

import com.ais.common.LoggerObject
import org.apache.hadoop.hbase.filter.{Filter, PrefixFilter}
import org.apache.hadoop.hbase.util.Bytes

/** Created by ShiGZ on 2016/12/12. */
object IPTool extends LoggerObject {

  val IP_DEFAULT_SEPARATOR = "."
  val IP_SEG_COUNT = 4
  val IP_MIN_VALUE = 0
  val IP_MAX_VALUE = 255

  def isValidIP(IP: String): Boolean = {
    IP_SEG_COUNT == getIntIpSegArr(IP).length
  }

  private def getIntIpSegArr(IP: String): Array[Int] = {
    val result = ArrayTool.getIntArrWithSeparator(IP, IP_DEFAULT_SEPARATOR)
    result
  }

  def getIpPreFixListByIndex(partitionIndex: Int, partitionCount: Int): util.ArrayList[Filter] = {
    val result = new util.ArrayList[Filter]()
    if (0 < partitionCount) {
      for (index <- IP_MIN_VALUE to IP_MAX_VALUE) {
        if (partitionIndex == getHashValueByPartitionCount(index, partitionCount)) {
          result.add(new PrefixFilter(Bytes.toBytes(StringTool.joinValues(index.toString.reverse, IP_DEFAULT_SEPARATOR))))
        }
      }
    }
    result
  }

  def getIpLastSegment(IP: String): Int = {
    val result = getIntIpSegArr(IP).last
    if (IP_MIN_VALUE <= result && result <= IP_MAX_VALUE) {
      result
    } else {
      throw new RuntimeException("从IP地址中获取到的最后一段不在地址范围内！", null)
    }
  }

  /** 根据分片总数计算传入的值应该落到哪个分片上
    *
    * @param value           value
    * @param  partitionCount partitionCount
    * @return
    */
  def getHashValueByPartitionCount(value: Int, partitionCount: Int): Int = {
    value % partitionCount
  }

  /** 根据分片总数计算传入的IP应该落到哪个分片上
    *
    * @param IP              IP
    * @param  partitionCount partitionCount
    * @return
    */
  def getHashValueByPartitionCount(IP: String, partitionCount: Int): Int = {
    (HashTool.hash(IP) % partitionCount).toInt
  }

}
