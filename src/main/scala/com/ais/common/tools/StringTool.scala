package com.ais.common.tools

/** Created by ShiGZ on 2017/5/9. */
object StringTool {

  def nonEmpty(value: String): Boolean = {
    notNull(value) && value.nonEmpty
  }

  def notNull(value: Any): Boolean = {
    value != null
  }

  def joinValues(values: List[Any]): String = {
    val result = new StringBuilder
    for (value <- values) {
      if (notNull(value)) {
        result.append(value)
      }
    }
    result.toString
  }

  def joinValues(values: Any*): String = {
    val result = new StringBuilder
    for (value <- values) {
      if (notNull(value)) {
        result.append(value)
      }
    }
    result.toString
  }

  def joinValuesWithSeparator(separator: Any, firstValue: Any, values: Any*): String = {
    val result = new StringBuilder
    result.append(firstValue)
    for (value <- values) {
      result.append(separator).append(value)
    }
    result.toString()
  }

}
