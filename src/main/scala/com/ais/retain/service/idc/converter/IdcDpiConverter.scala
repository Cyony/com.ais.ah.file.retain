package com.ais.retain.service.idc.converter

import com.ais.common.LoggerObject
import com.ais.common.container.MultiContainer
import com.ais.common.tools.StringTool
import com.ais.common.tools.fetcher.{MultiFetcher, TextFetcher, TimeFetcher}
import com.ais.common.tools.spiltter.Splitter
import com.ais.retain.creator.ObjectCreator
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/** Created by ShiGZ on 2017/5/9. */
class IdcDpiConverter(settings: MultiContainer) extends LoggerObject {

  def convert(idcDpiData: RDD[String]): RDD[(String, IdcDpiDetail)] = {
    idcDpiData.mapPartitions(iterator => {
      val resultArr = new ArrayBuffer[(String, IdcDpiDetail)]()
      val parser = ObjectCreator.createIdcDpiParser(settings)
      while (iterator.hasNext) {
        val idcDpiTupleOption = parser.parse(iterator.next())
        if (idcDpiTupleOption.isDefined) {
          resultArr.append(idcDpiTupleOption.get)
        }
      }
      resultArr.toIterator
    })
  }

}

class IdcDpiParser(fieldSplitter: Splitter,
                   privateIpFetcher: TextFetcher,
                   startTimeFetcher: TimeFetcher,
                   normalMsgFetcher: MultiFetcher) extends LoggerObject {

  def parse(idcDpiLog: String): Option[(String, IdcDpiDetail)] = {
    try {
      val fields = fieldSplitter.split(idcDpiLog)
      Some(privateIpFetcher.fetch(fields), new IdcDpiDetail(startTimeFetcher.fetch(fields), normalMsgFetcher.fetch(fields)))
    } catch {
      case ex: Exception =>
        errorLog(ex, "将Idc Dpi 日志: ", idcDpiLog, " 转换为可供匹配的元组时出现了异常！异常信息为: ", ex.getCause)
        None
    }
  }

}

class IdcDpiDetail(val startTime: Long, val normalMsg: String) extends
  LoggerObject {

  override def toString: String = {
    StringTool.joinValues("startTime: ", startTime, ", normalMsg: ", normalMsg)
  }

}