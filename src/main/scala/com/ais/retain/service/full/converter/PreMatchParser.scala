package com.ais.retain.service.full.converter

import com.ais.common.LoggerObject
import com.ais.common.tools.StringTool
import com.ais.common.tools.fetcher.{LongFetcher, MultiFetcher, TextFetcher}
import com.ais.common.tools.spiltter.Splitter

/** Created by ShiGZ on 2017/5/9. */
class PreMatchParser(fieldSplitter: Splitter,
                     keyMsgFetcher: MultiFetcher,
                     startTimeFetcher: LongFetcher,
                     endTimeFetcher: TextFetcher,
                     normalMsgFetcher: MultiFetcher) extends LoggerObject {

  def parse(preMatchLog: String): Option[(String, PreMatchDetail)] = {
    try {
      val fields = fieldSplitter.split(preMatchLog)
      Some(keyMsgFetcher.fetch(fields), new PreMatchDetail(startTimeFetcher.fetch(fields),endTimeFetcher.fetch(fields), normalMsgFetcher.fetch(fields)))
    } catch {
      case ex: Exception =>
        errorLog(ex, "将预匹配结果日志: ", preMatchLog, " 转换为可供匹配的元组时出现了异常！异常信息为: ", ex.getCause)
        None
    }
  }

}

class PreMatchDetail(val startTime: Long,val endTime:String, val normalMsg: String) extends LoggerObject {

  override def toString: String = {
    StringTool.joinValues("startTime: ", startTime, ", endTime: ", endTime, ", normalMsg: ", normalMsg)
  }

}