package com.ais.common.tools

import java.util.Properties

/** Created by ShiGZ on 2016/11/21. */
object PropertiesTool {

  /** 根据key从properties中获取可选项的值,无值时返回None
    *
    * @param properties properties
    * @param key key
    * @return
    */
  def getPropertyOption(properties: Properties, key: String): Option[String] = {
    if (properties.containsKey(key)) {
      val value = properties.getProperty(key).trim
      if (StringTool.nonEmpty(value)) {
        Some(value)
      } else {
        None
      }
    } else {
      None
    }
  }


  /** 根据key从properties中获取必填的值,无值时抛异常
    *
    * @param  properties properties
    * @param key key
    * @return
    */
  def getRequiredProperty(properties: Properties, key: String): String = {
    if (properties.containsKey(key)) {
      val value = properties.getProperty(key).trim
      if (StringTool.nonEmpty(value)) {
        value
      } else {
        throw new RuntimeException(StringTool.joinValues("获取不到必填项 ", key, " 的有效值!"))
      }
    } else {
      throw new RuntimeException(StringTool.joinValues("获取不到必填项 ", key, " 的值!"))
    }
  }

}
