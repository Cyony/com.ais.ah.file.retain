package com.ais.common.tools.fetcher

import com.ais.common.LoggerObject

/** Created by ShiGZ on 2017/3/6. */
abstract class TimeProcessor extends LoggerObject {

  def process(fields: Array[String]): (Long, Long, Long)

}

object TimeProcessor {

  def getInstance(startTimeIndexOption: Option[Int], endTimeIndexOption: Option[Int], durationIndexOption: Option[Int], validDigitsOption: Option[Int], formatOption: Option[String], retainDigitsOption: Option[Int]): TimeProcessor = {
    if (startTimeIndexOption.isDefined) {
      if (endTimeIndexOption.isDefined) {
         if (durationIndexOption.isDefined) {
           new EntireProcessor(TimeFetcher.getInstance(startTimeIndexOption.get,validDigitsOption,formatOption,retainDigitsOption),
             TimeFetcher.getInstance(endTimeIndexOption.get,validDigitsOption,formatOption,retainDigitsOption),
             LongFetcher.getInstance(durationIndexOption.get))

         } else {
           new NoDurationProcessor(TimeFetcher.getInstance(startTimeIndexOption.get,validDigitsOption,formatOption,retainDigitsOption),
             TimeFetcher.getInstance(endTimeIndexOption.get,validDigitsOption,formatOption,retainDigitsOption))
         }
      } else {
        if (durationIndexOption.isDefined) {
          new NoEndTimeProcessor(TimeFetcher.getInstance(startTimeIndexOption.get,validDigitsOption,formatOption,retainDigitsOption),
            LongFetcher.getInstance(durationIndexOption.get))
        } else {
          throw new RuntimeException("持续时间配置和结束时间配置不可同时为空!",null)
        }
      }
    } else {
      if (endTimeIndexOption.isDefined) {
        if (durationIndexOption.isDefined) {
          new NoStartTimeProcessor(TimeFetcher.getInstance(endTimeIndexOption.get,validDigitsOption,formatOption,retainDigitsOption),
            LongFetcher.getInstance(durationIndexOption.get))
        } else {
          throw new RuntimeException("开始时间配置和持续时间配置不可同时为空!",null)
        }
      } else {
        throw new RuntimeException("开始时间配置和结束时间配置不可同时为空!",null)
      }
    }
  }

}

class NoEndTimeProcessor(startTimeFetcher: TimeFetcher, durationFetcher: LongFetcher) extends TimeProcessor {

  override def process(fields: Array[String]): (Long, Long, Long) = {
    val startTime = startTimeFetcher.fetch(fields)
    val duration = durationFetcher.fetch(fields)
    (startTime, startTime + duration * 1000, duration)
  }

}

class NoStartTimeProcessor(endTimeFetcher: TimeFetcher, durationFetcher: LongFetcher) extends TimeProcessor {

  override def process(fields: Array[String]): (Long, Long, Long) = {
    val endTime = endTimeFetcher.fetch(fields)
    val duration = durationFetcher.fetch(fields)
    (endTime - duration * 1000, endTime, duration)
  }

}

class NoDurationProcessor(startTimeFetcher: TimeFetcher, endTimeFetcher: TimeFetcher) extends TimeProcessor {

  override def process(fields: Array[String]): (Long, Long, Long) = {
    val startTime = startTimeFetcher.fetch(fields)
    val endTime = endTimeFetcher.fetch(fields)
    (startTime, endTime, (endTime - startTime) / 1000)
  }

}

class EntireProcessor(startTimeFetcher: TimeFetcher, endTimeFetcher: TimeFetcher, durationFetcher: LongFetcher) extends TimeProcessor {

  override def process(fields: Array[String]): (Long, Long, Long) = {
    (startTimeFetcher.fetch(fields), endTimeFetcher.fetch(fields), durationFetcher.fetch(fields))
  }

}

