package com.ais.common.tools.fetcher

import com.ais.common.LoggerObject
import com.ais.common.tools.ArrayTool

/** Created by ShiGZ on 2017/2/16. */
class TextFetcher extends LoggerObject {

  def fetch(fields: Array[String]): String = {
    ""
  }

  def fetchOption(fields: Array[String]): Option[String] = {
    None
  }
}

class ExtendTextFetcher(index: Int) extends TextFetcher {

  override def fetch(fields: Array[String]): String = {
    fields(index)
  }

  override def fetchOption(fields: Array[String]): Option[String] = {
    ArrayTool.getTextOptionByIndex(fields, index)
  }

}

object TextFetcher {

  def getInstance(index: Int): TextFetcher = {
    new ExtendTextFetcher(index)
  }

  def getInstance(indexOption: Option[Int]): TextFetcher = {
    if (indexOption.isDefined) {
      new ExtendTextFetcher(indexOption.get)
    } else {
      new TextFetcher()
    }

  }

}








