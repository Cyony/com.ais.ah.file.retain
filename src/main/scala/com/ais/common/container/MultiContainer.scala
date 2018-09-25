package com.ais.common.container

import java.util
import java.util.Properties

import com.ais.common.LoggerObject
import com.ais.common.tools.{ArrayTool, PropertiesTool}

/** Created by ShiGZ on 2016/11/20. */
class MultiContainer extends LoggerObject {

  private val intMap = new util.HashMap[String, Int]()
  private val textMap = new util.HashMap[String, String]()
  private val intArrMap = new util.HashMap[String, Array[Int]]()

  /** 获取可选的Int类型value
    *
    * @param key key
    * @return
    */
  def getIntOptionByKey(key: String): Option[Int] = {
    if (intMap.containsKey(key)) {
      Some(intMap.get(key))
    } else {
      None
    }
  }

  /** 获取Int类型value
    *
    * @param key key
    * @return
    */
  def getIntValueByKey(key: String): Int = {
    intMap.get(key)
  }

  /** 设置Int类型value
    *
    * @param key   key
    * @param value value
    */
  protected def setValue(key: String, value: Int): Unit = {
    intMap.put(key, value)
  }

  /** 获取可选的String类型value
    *
    * @param key key
    * @return
    */
  def getTextOptionByKey(key: String): Option[String] = {
    if (textMap.containsKey(key)) {
      Some(textMap.get(key))
    } else {
      None
    }
  }

  /** 获取String类型value
    *
    * @param key key
    * @return
    */
  def getTextByKey(key: String): String = {
    textMap.get(key)
  }

  /** 设置String类型value
    *
    * @param key   key
    * @param value value
    */
  protected def setValue(key: String, value: String): Unit = {
    textMap.put(key, value)
  }

  /** 获取可选的Int数组value
    *
    * @param key key
    * @return
    */
  def getIntArrOptionByKey(key: String): Option[Array[Int]] = {
    if (intArrMap.containsKey(key)) {
      Some(intArrMap.get(key))
    } else {
      None
    }
  }

  /** 获取Int数组value
    *
    * @param key key
    * @return
    */
  def getIntArrByKey(key: String): Array[Int] = {
    intArrMap.get(key)
  }

  /** 设置Int数组value
    *
    * @param key   key
    * @param value value
    */
  protected def setValue(key: String, value: Array[Int]): Unit = {
    intArrMap.put(key, value)
  }


  def initIntValue(props: Properties, keys: String*): Unit = {
    for (key <- keys) {
      val intOption = PropertiesTool.getPropertyOption(props, key)
      if (intOption.isDefined) {
        this.setValue(key, intOption.get.toInt)
      }
    }

  }

  def initRequiredIntValue(props: Properties, keys: String*): Unit = {
    for (key <- keys) {
      this.setValue(key, PropertiesTool.getRequiredProperty(props, key).toInt)
    }
  }

  def initIntValue(key: String, value: Int): Unit = {
    this.setValue(key, value)
  }

  def initTextValue(props: Properties, keys: String*): Unit = {
    for (key <- keys) {
      val textOption = PropertiesTool.getPropertyOption(props, key)
      if (textOption.isDefined) {
        this.setValue(key, textOption.get)
      }
    }
  }

  def initTextValue(key: String, value: String): Unit = {
    this.setValue(key, value)
  }

  def initRequiredTextValue(props: Properties, keys: String*): Unit = {
    for (key <- keys) {
      this.setValue(key, PropertiesTool.getRequiredProperty(props, key))
    }
  }

  def initIntArr(props: Properties, separator: String, keys: String*): Unit = {
    for (key <- keys) {
      val textOption = PropertiesTool.getPropertyOption(props, key)
      if (textOption.isDefined) {
        this.setValue(key, ArrayTool.getIntArrWithSeparator(textOption.get, separator))
      } else {
        this.setValue(key, new Array[Int](0))
      }
    }
  }

  def initRequiredIntArr(props: Properties, separator: String, keys: String*): Unit = {
    for (key <- keys) {
      this.setValue(key, ArrayTool.getIntArrWithSeparator(PropertiesTool.getRequiredProperty(props, key), separator))
    }
  }

}
