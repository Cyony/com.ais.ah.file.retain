package com.ais.common

import com.ais.common.tools.StringTool
import org.slf4j.LoggerFactory

/** Created by ShiGZ on 2016/11/18. */
class LoggerObject extends Serializable {

  private val LOGGER = LoggerFactory.getLogger(getClass)

  /** 处理info级别的日志内容
    *
    * @param contents 日志内容
    */
  def infoLog(contents: Any*): Unit = {
    LOGGER.info(StringTool.joinValues(contents.toList))
  }

  /** 处理error级别并且抛出了异常的日志内容
    *
    * @param contents  日志内容
    * @param throwable 异常信息
    */
  def errorLog(throwable: Throwable, contents: Any*): Unit = {
    LOGGER.error(StringTool.joinValues(contents.toList), throwable)
  }

  /** 处理error级别日志内容
    *
    * @param contents 日志内容
    */
  def errorLog(contents: Any*): Unit = {
    LOGGER.error(StringTool.joinValues(contents.toList))
  }

}
