package com.ais.common.tools.fetcher

import java.text.SimpleDateFormat

import com.ais.common.LoggerObject

/** Created by ShiGZ on 2017/3/20. */
class TimeFetcher(index: Int, validDigits: Int) extends LoggerObject {

  def validTime(fields: Array[String]): String = {
    val time = fields(index)
    if (time.length != validDigits) {
      throw new RuntimeException("时间字段长度不是: " + validDigits, null)
    } else {
      time
    }
  }

  def fetch(fields: Array[String]): Long = {
    fetchMillis(validTime(fields))
  }

  def fetchMillis(time: String): Long = {
    time.toLong
  }

  def fetchOption(fields: Array[String]): Option[Long] = {
    try {
      Some(fetch(fields))
    } catch {
      case ex: Exception =>
        None
    }
  }

}

object TimeFetcher {

  val MILLIS_DIGITS = 13
  val SUFFIX_ELEMENT = "0"

  def getInstance(index: Int, validDigitsOption: Option[Int], formatOption: Option[String], retainDigitsOption: Option[Int]): TimeFetcher = {
    val validDigits = {
      if (validDigitsOption.isDefined) {
        validDigitsOption.get
      } else {
        MILLIS_DIGITS
      }
    }
    if (formatOption.isDefined) {
      val format = formatOption.get
      if (retainDigitsOption.isDefined) {
        new OverLengthTextTimeFetcher(index, validDigits, new SimpleDateFormat(format), retainDigitsOption.get)
      } else {
        new TextTimeFetcher(index, validDigits, new SimpleDateFormat(format))
      }
    } else {
      if (retainDigitsOption.isDefined) {
        val retainDigits = retainDigitsOption.get
        if (retainDigits >= MILLIS_DIGITS) {
          new OverLengthNumberTimeFetcher(index, validDigits, MILLIS_DIGITS)
        } else {
          val suffix = {
            val result = new StringBuilder
            for (index <- 0 until MILLIS_DIGITS - retainDigits) {
              result.append(SUFFIX_ELEMENT)
            }
            result.toString()
          }
          new ShortNumberTimeFetcher(index,validDigits,retainDigits,suffix)
        }

      } else {
        if (validDigits == MILLIS_DIGITS) {
          new TimeFetcher(index, validDigits)
        } else {
          val suffix = {
            val result = new StringBuilder
            for (index <- 0 until MILLIS_DIGITS - validDigits) {
              result.append(SUFFIX_ELEMENT)
            }
            result.toString()
          }
          new NumberTimeFetcher(index, validDigits, suffix)
        }
      }
    }
  }

}

class NumberTimeFetcher(index: Int, validDigits: Int, suffix: String) extends TimeFetcher(index, validDigits) {

  override def fetchMillis(time: String): Long = {
    (time + suffix).toLong
  }

}

class OverLengthNumberTimeFetcher(index: Int, validDigits: Int, retainDigits: Int) extends TimeFetcher(index, validDigits) {

  override def fetchMillis(time: String): Long = {
    time.substring(0, retainDigits).toLong
  }

}

class ShortNumberTimeFetcher(index: Int, validDigits: Int, retainDigits: Int, suffix: String) extends TimeFetcher(index, validDigits) {

  override def fetchMillis(time: String): Long = {
    ( time.substring(0, retainDigits) + suffix).toLong
  }

}

class TextTimeFetcher(index: Int, validDigits: Int, timeFormat: SimpleDateFormat) extends TimeFetcher(index, validDigits) {

  override def fetchMillis(time: String): Long = {
    timeFormat.parse(time).getTime
  }

}


class OverLengthTextTimeFetcher(index: Int, validDigits: Int, timeFormat: SimpleDateFormat, retainDigits: Int) extends TimeFetcher(index, validDigits) {

  override def fetchMillis(time: String): Long = {
    timeFormat.parse(time.substring(0, retainDigits)).getTime
  }

}