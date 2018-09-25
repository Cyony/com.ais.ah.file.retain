package com.ais.retain.service.pre.converter

import com.ais.common.LoggerObject
import com.ais.common.container.MultiContainer
import com.ais.common.tools.StringTool
import com.ais.common.tools.fetcher.{MultiFetcher, TextFetcher, TimeFetcher}
import com.ais.common.tools.spiltter.Splitter
import com.ais.retain.creator.ObjectCreator
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/** Created by ShiGZ on 2017/5/9. */
class NatConverter(settings: MultiContainer) extends LoggerObject {

  def convert(natData: RDD[String]): RDD[(String, NatDetail)] = {
    natData.mapPartitions(iterator => {
      val resultArr = new ArrayBuffer[(String, NatDetail)]()
      val parser = ObjectCreator.createNatParser(settings)
      while (iterator.hasNext) {
        val natTupleOption = parser.parse(iterator.next())
        if (natTupleOption.isDefined) {
          resultArr.append(natTupleOption.get)
        }
      }
      resultArr.toIterator
    })
  }

}

class NatParser(fieldSplitter: Splitter,
                privateIpFetcher:TextFetcher,
                startTimeFetcher: TimeFetcher,
                endTimeFetcher: TimeFetcher,
                keyMsgFetcher: MultiFetcher,
                normalMsgFetcher: MultiFetcher) extends LoggerObject {

  def parse(natLog: String): Option[(String, NatDetail)] = {
    try {
      val fields = fieldSplitter.split(natLog)
      Some(privateIpFetcher.fetch(fields), new NatDetail(startTimeFetcher.fetch(fields), endTimeFetcher.fetch(fields),keyMsgFetcher.fetch(fields), normalMsgFetcher.fetch(fields)))
    } catch {
      case ex: Exception =>
        errorLog(ex, "将NAT日志: ", natLog, " 转换为可供匹配的元组时出现了异常！异常信息为: ", ex.getCause)
        None
    }
  }

}

class NatDetail(val startTime: Long, val endTime: Long, val keyMsg: String, val normalMsg: String) extends LoggerObject {

  override def toString: String = {
    StringTool.joinValues("startTime: ", startTime, ", endTime: ", endTime, ", keyMsg: ", keyMsg, ", normalMsg: ", normalMsg)
  }

}