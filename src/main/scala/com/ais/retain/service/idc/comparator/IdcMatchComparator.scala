package com.ais.retain.service.idc.comparator

import com.ais.retain.service.idc.converter.IdcDpiDetail
import com.ais.retain.service.pre.converter.RadiusDetail

/** Created by ShiGZ on 2017/5/10. */
object IdcMatchComparator {

  def isMatch(radiusDetail: RadiusDetail, idcDpiDetail: IdcDpiDetail): Boolean = {
    radiusDetail.startTime <= idcDpiDetail.startTime
  }

}
