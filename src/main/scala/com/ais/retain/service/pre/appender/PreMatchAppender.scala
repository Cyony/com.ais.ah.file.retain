package com.ais.retain.service.pre.appender

import com.ais.common.LoggerObject
import com.ais.common.tools.StringTool
import com.ais.retain.service.pre.converter.{NatDetail, RadiusDetail}

/** Created by ShiGZ on 2017/5/10. */
class PreMatchAppender(separator: String, defaultValue: String) extends LoggerObject {

  def appendMsg(radiusDetail: RadiusDetail, natDetail: NatDetail): String = {
    StringTool.joinValuesWithSeparator(separator, natDetail.startTime, natDetail.endTime, natDetail.normalMsg, radiusDetail.startTime, radiusDetail.normalMsg)
  }

  def appendPublicKeyMsg(privateIp: String, natDetail: NatDetail): String = {
    StringTool.joinValuesWithSeparator(separator, privateIp, natDetail.keyMsg)
  }

}