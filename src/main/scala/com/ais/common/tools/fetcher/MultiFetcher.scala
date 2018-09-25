package com.ais.common.tools.fetcher

import com.ais.common.LoggerObject
import com.ais.common.tools.ArrayTool

/** Created by ShiGZ on 2017/3/20. */
class MultiFetcher(separator: Any) extends LoggerObject {

  def fetch(fields: Array[String]): String = {
    ""
  }

  def fetchWithPrefixes(fields: Array[String], firstPrefix: Any, prefixes: Any*): String = {
    val resultBuilder = new StringBuilder
    resultBuilder.append(firstPrefix)
    for (prefix <- prefixes) {
      resultBuilder.append(separator).append(prefix)
    }
    resultBuilder.toString()
  }

}

class OneFetcher(separator: Any, index: Int) extends MultiFetcher(separator) {

  override def fetch(fields: Array[String]): String = {
    fields(index)
  }

  override def fetchWithPrefixes(fields: Array[String], firstPrefix: Any, prefixes: Any*): String = {
    val resultBuilder = new StringBuilder
    resultBuilder.append(firstPrefix)
    for (prefix <- prefixes) {
      resultBuilder.append(separator).append(prefix)
    }
    resultBuilder.append(separator)
    val firstTextOption = ArrayTool.getTextOptionByIndex(fields, index)
    if (firstTextOption.isDefined) {
      resultBuilder.append(firstTextOption.get)
    }
    resultBuilder.toString()
  }

}

class MoreThanOneFetcher(separator: Any, firstIndex: Int, indexes: Array[Int]) extends MultiFetcher(separator) {

  override def fetch(fields: Array[String]): String = {
    val resultBuilder = new StringBuilder
    val firstTextOption = ArrayTool.getTextOptionByIndex(fields, firstIndex)
    if (firstTextOption.isDefined) {
      resultBuilder.append(firstTextOption.get)
    }
    for (index <- indexes) {
      resultBuilder.append(separator)
      val textOption = ArrayTool.getTextOptionByIndex(fields, index)
      if (textOption.isDefined) {
        resultBuilder.append(textOption.get)
      }
    }
    resultBuilder.toString()
  }

  override def fetchWithPrefixes(fields: Array[String], firstPrefix: Any, prefixes: Any*): String = {
    val resultBuilder = new StringBuilder
    resultBuilder.append(firstPrefix)

    for (prefix <- prefixes) {
      resultBuilder.append(separator).append(prefix)
    }

    resultBuilder.append(separator)
    val firstTextOption = ArrayTool.getTextOptionByIndex(fields, firstIndex)
    if (firstTextOption.isDefined) {
      resultBuilder.append(firstTextOption.get)
    }

    for (index <- indexes) {
      resultBuilder.append(separator)
      val textOption = ArrayTool.getTextOptionByIndex(fields, index)
      if (textOption.isDefined) {
        resultBuilder.append(textOption.get)
      }
    }
    resultBuilder.toString()
  }

}

object MultiFetcher {

  def getInstance(indexes: Array[Int], separator: Any): MultiFetcher = {
    val indexCount = indexes.length
    if (0 == indexCount) {
      new MultiFetcher(separator)
    } else if (1 == indexCount) {
      new OneFetcher(separator, indexes(0))
    } else {
      new MoreThanOneFetcher(separator, indexes(0), indexes.drop(1))
    }
  }

}

