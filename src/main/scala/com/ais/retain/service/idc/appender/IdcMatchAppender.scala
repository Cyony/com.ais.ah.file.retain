package com.ais.retain.service.idc.appender

import com.ais.common.LoggerObject
import com.ais.common.tools.StringTool
import com.ais.retain.service.idc.converter.IdcDpiDetail
import com.ais.retain.service.pre.converter.RadiusDetail

/** Created by ShiGZ on 2017/5/10. */
class IdcMatchAppender(separator: String, msgHead: String, defaultValue: String) extends LoggerObject {

  def append(privateIp: String, radiusDetail: RadiusDetail, idcDpiDetail: IdcDpiDetail): String = {
    StringTool.joinValuesWithSeparator(separator, msgHead,privateIp, radiusDetail.startTime, radiusDetail.normalMsg, idcDpiDetail.startTime, idcDpiDetail.normalMsg)
  }
}