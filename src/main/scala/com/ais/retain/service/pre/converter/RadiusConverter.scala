package com.ais.retain.service.pre.converter

import java.util

import com.ais.common.LoggerObject
import com.ais.common.tools.StringTool
import com.ais.common.tools.fetcher.{MultiFetcher, TimeFetcher}
import com.ais.common.tools.spiltter.Splitter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ConnectionFactory, Scan}

import scala.collection.mutable.ArrayBuffer

/** Created by ShiGZ on 2017/5/9. */
class RadiusConverter(parser: RadiusParser,
                      config: Configuration,
                      tableName: TableName,
                      family: Array[Byte],
                      detailColumn: Array[Byte],
                      scan: Scan) extends LoggerObject {

  def convert(): util.HashMap[String, ArrayBuffer[RadiusDetail]] = {
    val result = new util.HashMap[String, ArrayBuffer[RadiusDetail]]
    val connection = ConnectionFactory.createConnection(config)
    val table = connection.getTable(tableName)
    val rs = table.getScanner(scan)
    try {
      val rsIt = rs.iterator()
      while (rsIt.hasNext) {
        val radiusResult = rsIt.next()
        val radiusTupleOption = parser.parse(new Predef.String(radiusResult.getValue(family, detailColumn)), new Predef.String(radiusResult.getRow))
        if (radiusTupleOption.isDefined) {
          val (privateIp, radiusDetail) = radiusTupleOption.get
          if (result.containsKey(privateIp)) {
            val radiusDetailAB = result.get(privateIp)
            radiusDetailAB.append(radiusDetail)
            result.put(privateIp, radiusDetailAB.sortWith(_.startTime > _.startTime))
          } else {
            val radiusDetailAB = new ArrayBuffer[RadiusDetail]()
            radiusDetailAB.append(radiusDetail)
            result.put(privateIp, radiusDetailAB)
          }
        }
      }
    } catch {
      case ex: Exception =>
        throw new RuntimeException("从 HBase 中获取 Radius 信息并转换为可匹配元组时出现了异常,异常信息为: " + ex.getCause, ex)
    } finally {
      rs.close()
      table.close()
      connection.close()
    }
    result
  }

}

class RadiusParser(fieldSplitter: Splitter,
                   rowKeySplitter: Splitter,
                   endTimeFetcher: TimeFetcher,
                   normalMsgFetcher: MultiFetcher) extends LoggerObject {

  def parse(radiusLog: String, radiusRowKey: String): Option[(String, RadiusDetail)] = {
    try {
      val fields = fieldSplitter.split(radiusLog)
      val Array(username, startTime, privateIp) = rowKeySplitter.split(radiusRowKey.reverse)
      Some(privateIp, new RadiusDetail(startTime.toLong,endTimeFetcher.fetchOption(fields), normalMsgFetcher.fetchWithPrefixes(fields, username)))
    } catch {
      case ex: Exception =>
        errorLog(ex, "将 Radius日志: ", radiusLog, " rowKey: ", radiusRowKey, " 转换为可供匹配的元组时出现了异常！异常信息为: ", ex.getCause)
        None
    }
  }

}

class RadiusDetail(val startTime: Long,val endTimeOption:Option[Long], val normalMsg: String) extends LoggerObject {

  override def toString: String = {
    StringTool.joinValues("startTime: ", startTime, ", endTime: ", endTimeOption, ", normalMsg: ", normalMsg)
  }

}
