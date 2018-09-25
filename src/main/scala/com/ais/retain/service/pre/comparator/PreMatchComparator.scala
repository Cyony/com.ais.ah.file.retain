package com.ais.retain.service.pre.comparator

import com.ais.retain.service.pre.converter.{NatDetail, RadiusDetail}

/** Created by ShiGZ on 2017/5/10. */
object PreMatchComparator {

  def isMatch(radiusDetail: RadiusDetail, natDetail: NatDetail): Boolean = {
    if (radiusDetail.endTimeOption.isDefined) {
      radiusDetail.startTime <= natDetail.startTime && natDetail.startTime <= radiusDetail.endTimeOption.get
    } else {
      radiusDetail.startTime <= natDetail.startTime
    }

  }

}