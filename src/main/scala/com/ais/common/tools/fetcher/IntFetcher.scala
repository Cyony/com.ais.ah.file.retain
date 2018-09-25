package com.ais.common.tools.fetcher

import com.ais.common.LoggerObject

/** Created by ShiGZ on 2017/3/20. */
class IntFetcher(index: Int) extends LoggerObject {

  def fetch(fields: Array[String]): Int = {
    fields(index).toInt
  }

  def fetchOption(fields: Array[String]): Option[Int] = {
    try {
      Some(fetch(fields))
    } catch {
      case ex: Exception =>
        None
    }
  }

}

object IntFetcher {

  def getInstance(index: Int): IntFetcher = {
    new IntFetcher(index)
  }

}
