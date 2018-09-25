package com.ais.retain.service.full.appender

import com.ais.common.LoggerObject
import com.ais.common.tools.StringTool
import com.ais.retain.service.full.converter.{DpiDetail, PreMatchDetail}

/** Created by ShiGZ on 2017/5/10. */
class FullMatchAppender(separator: String, msgHead: String, defaultValue: String) extends LoggerObject {

  def append(keyMsg: String, preMatchDetail: PreMatchDetail,dpiDetail: DpiDetail): String = {
    StringTool.joinValuesWithSeparator(separator, msgHead, keyMsg,dpiDetail.startTime,dpiDetail.endTime,dpiDetail.normalMsg,preMatchDetail.startTime,preMatchDetail.endTime,preMatchDetail.normalMsg)
  }

}