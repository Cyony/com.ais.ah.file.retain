package com.ais.retain.service.full.comparator

import com.ais.retain.service.full.converter.{DpiDetail, PreMatchDetail}

/** Created by ShiGZ on 2017/5/10. */
object FullMatchComparator {

  def isMatch(preMatchDetail: PreMatchDetail, dpiDetail: DpiDetail, lagMillis: Int): Boolean = {
    dpiDetail.startTime - lagMillis <= preMatchDetail.startTime && preMatchDetail.startTime <= dpiDetail.endTime + lagMillis
  }

}