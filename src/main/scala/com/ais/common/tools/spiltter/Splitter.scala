package com.ais.common.tools.spiltter

import com.ais.common.LoggerObject
import com.ais.common.tools.ArrayTool


/** Created by ShiGZ on 2017/2/16. */
class Splitter(separator: String) extends LoggerObject {

  def split(text: String): Array[String] = {
    ArrayTool.getTextArrWithSeparator(text, separator)
  }

}

object Splitter {

  def getInstance(separator: String): Splitter = {
    new Splitter(separator)
  }

}

