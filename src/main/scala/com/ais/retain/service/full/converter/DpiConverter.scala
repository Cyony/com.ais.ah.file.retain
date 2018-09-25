package com.ais.retain.service.full.converter

import com.ais.common.LoggerObject
import com.ais.common.container.MultiContainer
import com.ais.common.tools.StringTool
import com.ais.common.tools.fetcher.{MultiFetcher, TextFetcher, TimeFetcher}
import com.ais.common.tools.spiltter.Splitter
import com.ais.retain.creator.ObjectCreator
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/** Created by ShiGZ on 2017/5/9. */
class DpiConverter(settings: MultiContainer) extends LoggerObject {

  def convert(dpiData: RDD[String]): RDD[(String, DpiDetail)] = {
    dpiData.mapPartitions(iterator => {
      val resultArr = new ListBuffer[(String, DpiDetail)]()
      val parser = ObjectCreator.createDpiParser(settings)
      while (iterator.hasNext) {
        val dpiTupleOption = parser.parse(iterator.next())
        if (dpiTupleOption.isDefined) {
          resultArr.append(dpiTupleOption.get)
        }
      }
      resultArr.toIterator
    })
  }
}

class DpiParser(fieldSplitter: Splitter,
                startTimeFetcher: TimeFetcher,
                endTimeFetcher: TimeFetcher,
                keyMsgFetcher: MultiFetcher,
                normalMsgFetcher: MultiFetcher) extends LoggerObject {
  def parse(dpiLog: String): Option[(String, DpiDetail)] = {
    try {
      val fields = fieldSplitter.split(dpiLog)
      Some(keyMsgFetcher.fetch(fields), new DpiDetail(startTimeFetcher.fetch(fields), endTimeFetcher.fetch(fields), normalMsgFetcher.fetch(fields)))
    } catch {
      case ex: Exception =>
        errorLog(ex, "将 DPI日志: ", dpiLog, " 转换为可供匹配的元组时出现了异常！异常信息为: ", ex.getCause)
        None
    }
  }

}


class DpiDetail(val startTime: Long, val endTime: Long, val normalMsg: String) extends
  LoggerObject {

  override def toString: String = {
    StringTool.joinValues("startTime: ", startTime, ", endTime: ", endTime, ", normalMsg: ", normalMsg)
  }

}

