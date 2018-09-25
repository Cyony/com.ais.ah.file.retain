package com.ais.common.tools

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.ais.common.LoggerObject

/** Created by ShiGZ on 2016/8/25. */
object TimeTool extends LoggerObject {

  private val DEFAULT_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  /** 根据格式获得时间格式化器
    *
    * @param format format
    * @return
    */
  def getTimeFormat(format: String): SimpleDateFormat = {
    new SimpleDateFormat(format)
  }

  /** 获得默认格式的当前时间
    *
    * @return
    */
  def currentTime(): String = {
    DEFAULT_DATE_FORMAT.format(new Date())
  }

  /** 使用输入的时间格式化器，将输入时间转换成Long型毫秒数
    *
    * @param textTime   textTime
    * @param timeFormat timeFormat
    * @return
    */
  def getMilliSeconds(textTime: String, timeFormat: SimpleDateFormat): Long = {
    timeFormat.parse(textTime).getTime
  }

  /** 使用输入的时间格式化器，将输入时间转换成Long型毫秒数,由于有超过格式化器识别长度的现象，所以需要对时间字段进行截取
    *
    * @param textTime         textTime
    * @param timeFormat       timeFormat
    * @param timeFormatLength timeFormatLength
    * @return
    */
  def getMilliSeconds(textTime: String, timeFormat: SimpleDateFormat, timeFormatLength: Int): Long = {
    timeFormat.parse(textTime.substring(0, timeFormatLength)).getTime
  }

  /** 使用输入的时间格式化器将输入时间格式化成Long型毫秒数，如果转换失败，返回None
    *
    * @param textTimeOption textTimeOption
    * @param timeFormat     timeFormat
    * @return
    */
  def getMilliSecondsOption(textTimeOption: Option[String], timeFormat: SimpleDateFormat): Option[Long] = {
    if (textTimeOption.isDefined && textTimeOption.get.nonEmpty) {
      val time = textTimeOption.get
      try {
        Some(getMilliSeconds(time, timeFormat))
      } catch {
        case ex: Exception => errorLog(ex, "将时间 ", time, " 格式化为毫秒数时出现了异常！异常信息为:", ex.getCause)
          None
      }
    } else {
      None
    }
  }

  /** 使用输入的时间格式化器将输入时间格式化成Long型毫秒数，如果转换失败，返回None,由于有超过格式化器识别长度的现象，所以需要对时间字段进行截取
    *
    * @param textTimeOption   textTimeOption
    * @param timeFormat       timeFormat
    * @param timeFormatLength timeFormatLength
    * @return
    */
  def getMilliSecondsOption(textTimeOption: Option[String], timeFormat: SimpleDateFormat, timeFormatLength: Int): Option[Long] = {
    if (textTimeOption.isDefined && textTimeOption.get.nonEmpty) {
      val time = textTimeOption.get
      try {
        Some(getMilliSeconds(time, timeFormat, timeFormatLength))
      } catch {
        case ex: Exception => errorLog(ex, "将时间 ", time, " 格式化为毫秒数时出现了异常！异常信息为:", ex.getCause)
          None
      }
    } else {
      None
    }
  }

  /** 获得当前日期是今年第几天
    *
    * @return
    */
  def currentDayOfYear(): Int = {
    Calendar.getInstance().get(Calendar.DAY_OF_YEAR)
  }

  /** 获得当前时间是今天第几个小时
    *
    * @return
    */
  def currentHourOfDay(): Int = {
    Calendar.getInstance().get(Calendar.HOUR_OF_DAY)
  }

}
