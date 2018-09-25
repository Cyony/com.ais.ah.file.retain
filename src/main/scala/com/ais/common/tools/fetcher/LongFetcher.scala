package com.ais.common.tools.fetcher

import com.ais.common.LoggerObject

/** Created by ShiGZ on 2017/3/20. */
class LongFetcher(index: Int) extends LoggerObject {

  def fetch(fields: Array[String]): Long = {
    fields(index).toLong
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

object LongFetcher {

  def getInstance(index: Int): LongFetcher = {
    new LongFetcher(index)
  }

}
